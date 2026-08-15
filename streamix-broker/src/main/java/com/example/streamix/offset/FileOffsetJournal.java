package com.example.streamix.offset;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.function.BiConsumer;

import com.example.streamix.storage.JsonLines;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// Commits append to offsets.log; snapshots compact everything into offsets.snapshot and empty the log.
public class FileOffsetJournal implements OffsetJournal {

	public record Entry(String group, String topic, int partition, long offset) {}

	private final ObjectMapper mapper = JsonMapper.builder().build();
	private final Path journalFile;
	private final Path snapshotFile;
	private BufferedWriter out; // guarded by this

	public FileOffsetJournal(Path dataDir) {
		this.journalFile = dataDir.resolve("offsets.log");
		this.snapshotFile = dataDir.resolve("offsets.snapshot");
	}

	@Override
	public synchronized void append(GroupTopicPartition key, long offset) {
		try {
			if (out == null) out = JsonLines.openAppend(journalFile);
			out.write(mapper.writeValueAsString(new Entry(key.group(), key.topic(), key.partition(), offset)));
			out.write('\n');
			out.flush();
		} catch (IOException e) {
			throw new UncheckedIOException("offset journal append failed", e);
		}
	}

	// Callers must guarantee `state` covers every append made so far (OffsetStore serializes both).
	@Override
	public synchronized void snapshot(Map<GroupTopicPartition, Long> state) {
		try {
			Files.createDirectories(snapshotFile.getParent());
			StringBuilder content = new StringBuilder();
			for (Map.Entry<GroupTopicPartition, Long> e : state.entrySet()) {
				GroupTopicPartition k = e.getKey();
				content.append(mapper.writeValueAsString(new Entry(k.group(), k.topic(), k.partition(), e.getValue())))
						.append('\n');
			}
			Path tmp = snapshotFile.resolveSibling("offsets.snapshot.tmp");
			Files.writeString(tmp, content.toString());
			Files.move(tmp, snapshotFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
			if (out != null) {
				out.close();
				out = null;
			}
			Files.deleteIfExists(journalFile); // the snapshot now owns all durable state
		} catch (IOException e) {
			throw new UncheckedIOException("offset snapshot failed", e);
		}
	}

	// Snapshot first, then journal entries on top (last write wins in the store).
	@Override
	public void replay(BiConsumer<GroupTopicPartition, Long> sink) {
		try {
			readInto(snapshotFile, sink);
			readInto(journalFile, sink);
		} catch (IOException e) {
			throw new UncheckedIOException("offset journal replay failed", e);
		}
	}

	private void readInto(Path file, BiConsumer<GroupTopicPartition, Long> sink) throws IOException {
		JsonLines.replay(file, line -> {
			Entry e = mapper.readValue(line, Entry.class);
			sink.accept(new GroupTopicPartition(e.group(), e.topic(), e.partition()), e.offset());
		});
	}

	// Spring auto-invokes close() on context shutdown.
	public synchronized void close() {
		try {
			if (out != null) out.close();
		} catch (IOException ignored) {
			// best-effort close on shutdown
		}
	}
}
