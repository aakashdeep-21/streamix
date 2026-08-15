package com.example.streamix.storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.streamix.core.Message;

import tools.jackson.databind.ObjectMapper;

// One partition as rolling JSON-lines segments: metadata under the monitor, file reads lock-free.
final class SegmentedPartitionLog {

	private static final Logger log = LoggerFactory.getLogger(SegmentedPartitionLog.class);
	private static final int INDEX_INTERVAL = 64;

	private final Path dir;
	private final ObjectMapper mapper;
	private final long segmentMaxBytes;
	private final boolean fsyncEveryAppend;

	private final List<Segment> segments = new ArrayList<>(); // guarded by this
	private volatile long nextOffset = 0;
	private volatile long beginOffset = 0;
	private FileOutputStream activeFile; // guarded by this
	private BufferedOutputStream activeOut; // guarded by this

	// Sparse index holds the byte position of every 64th record: a read seeks then skips <64 lines.
	private static final class Segment {
		final long baseOffset;
		final Path file;
		final List<Long> index = new ArrayList<>();
		long sizeBytes;
		int count;
		long lastTimestampMs;

		Segment(long baseOffset, Path file) {
			this.baseOffset = baseOffset;
			this.file = file;
		}
	}

	private record ReadPlan(Path file, long startPos, int skipLines, int take) {}

	SegmentedPartitionLog(Path dir, ObjectMapper mapper, long segmentMaxBytes, boolean fsyncEveryAppend) {
		this.dir = dir;
		this.mapper = mapper;
		this.segmentMaxBytes = segmentMaxBytes;
		this.fsyncEveryAppend = fsyncEveryAppend;
	}

	// Startup-only: rebuild segment metadata + sparse indexes; only the final segment may self-heal.
	synchronized void recover() {
		try {
			Files.createDirectories(dir);
			List<Path> files;
			try (Stream<Path> s = Files.list(dir)) {
				files = s.filter(p -> p.getFileName().toString().endsWith(".log")).sorted().toList();
			}
			long expected = -1;
			for (int i = 0; i < files.size(); i++) {
				Path file = files.get(i);
				Segment seg = new Segment(baseOffsetOf(file), file);
				if (expected >= 0 && seg.baseOffset != expected) {
					throw new IllegalStateException("segment gap in " + dir + ": expected base " + expected + ", found " + seg.baseOffset);
				}
				boolean lastSegment = i == files.size() - 1;
				seg.sizeBytes = JsonLines.replay(file, lastSegment, (line, pos) -> {
					Message m = mapper.readValue(line, Message.class);
					if (m.offset() != seg.baseOffset + seg.count) {
						throw new IllegalStateException("non-contiguous offset " + m.offset() + " in " + file);
					}
					if (seg.count % INDEX_INTERVAL == 0) seg.index.add(pos);
					seg.count++;
					seg.lastTimestampMs = Math.max(seg.lastTimestampMs, m.timestamp());
				});
				segments.add(seg);
				expected = seg.baseOffset + seg.count;
			}
			if (!segments.isEmpty()) {
				beginOffset = segments.get(0).baseOffset;
				nextOffset = expected;
				log.debug("recovered {}: {} segment(s), offsets [{}, {})", dir, segments.size(), beginOffset, nextOffset);
			}
		} catch (IOException e) {
			throw new UncheckedIOException("recovery failed for " + dir, e);
		}
	}

	synchronized Message append(String key, Object value, Map<String, String> headers) {
		try {
			Message m = new Message(nextOffset, key, value, headers, System.currentTimeMillis());
			byte[] line = mapper.writeValueAsBytes(m);
			if (segments.isEmpty()) {
				startSegment(nextOffset);
			} else if (activeOut == null) {
				openActive(); // first append after recovery continues the last segment
			}
			Segment active = segments.get(segments.size() - 1);
			if (active.sizeBytes > 0 && active.sizeBytes + line.length + 1 > segmentMaxBytes) {
				startSegment(nextOffset);
				active = segments.get(segments.size() - 1);
			}
			if (active.count % INDEX_INTERVAL == 0) active.index.add(active.sizeBytes);
			activeOut.write(line);
			activeOut.write('\n');
			activeOut.flush(); // into the OS page cache: readers and process crashes are covered
			if (fsyncEveryAppend) activeFile.getFD().sync();
			active.sizeBytes += line.length + 1;
			active.count++;
			active.lastTimestampMs = Math.max(active.lastTimestampMs, m.timestamp());
			nextOffset = m.offset() + 1; // published only after the bytes are readable
			return m;
		} catch (IOException e) {
			throw new UncheckedIOException("append failed in " + dir, e);
		}
	}

	List<Message> read(long fromOffset, int max) {
		List<Message> out = new ArrayList<>();
		long cursor = Math.max(fromOffset, beginOffset);
		int retries = 0;
		while (out.size() < max) {
			ReadPlan plan;
			synchronized (this) {
				if (cursor < beginOffset) cursor = beginOffset; // re-clamp after a trim race
				if (cursor >= nextOffset) break;
				Segment seg = segmentFor(cursor);
				long rel = cursor - seg.baseOffset;
				plan = new ReadPlan(seg.file, seg.index.get((int) (rel / INDEX_INTERVAL)),
						(int) (rel % INDEX_INTERVAL), (int) Math.min(seg.count - rel, (long) (max - out.size())));
			}
			try (InputStream in = new BufferedInputStream(Files.newInputStream(plan.file()), 64 * 1024)) {
				in.skipNBytes(plan.startPos());
				for (int i = 0; i < plan.skipLines(); i++) skipLine(in);
				int taken = 0;
				for (; taken < plan.take(); taken++) {
					byte[] line = readLine(in);
					if (line == null) break;
					out.add(mapper.readValue(line, Message.class));
					cursor++;
				}
				if (taken < plan.take()) break; // hit EOF early; never spin
			} catch (NoSuchFileException e) {
				if (++retries > 3) break; // segment trimmed underneath us; loop re-clamps to beginOffset
			} catch (IOException e) {
				throw new UncheckedIOException("read failed in " + dir, e);
			}
		}
		return out;
	}

	// Deletes whole sealed segments only; the active segment is never trimmed.
	synchronized long enforceRetention(long minTimestampMs, long maxBytes) {
		long removed = 0;
		while (segments.size() > 1 && segments.get(0).lastTimestampMs < minTimestampMs) {
			removed += dropOldest();
		}
		if (maxBytes > 0) {
			long total = segments.stream().mapToLong(s -> s.sizeBytes).sum();
			while (segments.size() > 1 && total > maxBytes) {
				total -= segments.get(0).sizeBytes;
				removed += dropOldest();
			}
		}
		return removed;
	}

	synchronized void sync() {
		try {
			if (activeOut != null) {
				activeOut.flush();
				activeFile.getFD().sync();
			}
		} catch (IOException e) {
			throw new UncheckedIOException("fsync failed in " + dir, e);
		}
	}

	long endOffset() { return nextOffset; }

	long beginOffset() { return beginOffset; }

	synchronized long sizeBytes() {
		return segments.stream().mapToLong(s -> s.sizeBytes).sum();
	}

	synchronized int segmentCount() { return segments.size(); }

	synchronized void close() {
		try {
			if (activeOut != null) activeOut.close();
		} catch (IOException ignored) {
			// best-effort close on shutdown/delete
		}
		activeOut = null;
		activeFile = null;
	}

	private long dropOldest() {
		Segment victim = segments.remove(0);
		try {
			Files.deleteIfExists(victim.file);
		} catch (IOException e) {
			log.warn("could not delete trimmed segment {}", victim.file, e);
		}
		beginOffset = segments.get(0).baseOffset;
		log.info("trimmed segment {} ({} messages) from {}", victim.file.getFileName(), victim.count, dir);
		return victim.count;
	}

	// Floor lookup by base offset; callers guarantee beginOffset <= offset < nextOffset.
	private Segment segmentFor(long offset) {
		int lo = 0, hi = segments.size() - 1;
		Segment result = segments.get(0);
		while (lo <= hi) {
			int mid = (lo + hi) >>> 1;
			if (segments.get(mid).baseOffset <= offset) {
				result = segments.get(mid);
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}
		return result;
	}

	private void openActive() throws IOException {
		Segment last = segments.get(segments.size() - 1);
		JsonLines.repairTrailingNewline(last.file);
		activeFile = new FileOutputStream(last.file.toFile(), true);
		activeOut = new BufferedOutputStream(activeFile);
	}

	private void startSegment(long baseOffset) throws IOException {
		close();
		Path file = dir.resolve(String.format("%020d.log", baseOffset));
		segments.add(new Segment(baseOffset, file));
		activeFile = new FileOutputStream(file.toFile(), true);
		activeOut = new BufferedOutputStream(activeFile);
		if (segments.size() > 1) log.info("rolled {} to new segment at offset {}", dir, baseOffset);
	}

	private static long baseOffsetOf(Path file) {
		String name = file.getFileName().toString();
		return Long.parseLong(name.substring(0, name.length() - ".log".length()));
	}

	private static byte[] readLine(InputStream in) throws IOException {
		ByteArrayOutputStream buf = new ByteArrayOutputStream(256);
		int b;
		while ((b = in.read()) != -1) {
			if (b == '\n') return buf.toByteArray();
			buf.write(b);
		}
		return buf.size() > 0 ? buf.toByteArray() : null;
	}

	private static void skipLine(InputStream in) throws IOException {
		int b;
		do {
			b = in.read();
		} while (b != -1 && b != '\n');
	}
}
