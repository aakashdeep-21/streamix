package com.example.streamix.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.streamix.core.Message;
import com.example.streamix.core.TopicMetadata;
import com.example.streamix.core.TopicPartition;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// JSON-lines write-through log: memory serves reads, files make every ack'd append durable.
public class FileLogStorage implements LogStorage {

	private static final Logger log = LoggerFactory.getLogger(FileLogStorage.class);

	private final InMemoryLogStorage memory = new InMemoryLogStorage();
	private final ObjectMapper mapper = JsonMapper.builder().build();
	private final Path topicsDir;
	private final ConcurrentHashMap<TopicPartition, LogWriter> writers = new ConcurrentHashMap<>();
	private final List<TopicMetadata> recovered = new ArrayList<>();

	public FileLogStorage(Path dataDir) {
		this.topicsDir = dataDir.resolve("topics");
	}

	// Startup-only: rebuild in-memory state from disk before the broker serves traffic.
	public void recover() {
		try {
			Files.createDirectories(topicsDir);
			try (Stream<Path> dirs = Files.list(topicsDir)) {
				for (Path dir : dirs.filter(Files::isDirectory).sorted().toList()) {
					recoverTopic(dir);
				}
			}
		} catch (IOException e) {
			throw new UncheckedIOException("log recovery failed", e);
		}
	}

	private void recoverTopic(Path dir) throws IOException {
		Path metaFile = dir.resolve("meta.json");
		if (!Files.exists(metaFile)) {
			log.warn("skipping {}: no meta.json", dir);
			return;
		}
		TopicMetadata meta = mapper.readValue(Files.readString(metaFile), TopicMetadata.class);
		memory.createLog(meta);
		for (int p = 0; p < meta.partitions(); p++) {
			final int partition = p;
			JsonLines.replay(partitionFile(meta.name(), p),
					line -> memory.restore(meta.name(), partition, mapper.readValue(line, Message.class)));
		}
		recovered.add(meta);
		log.info("recovered topic '{}' ({} partitions)", meta.name(), meta.partitions());
	}

	@Override
	public List<TopicMetadata> recoveredTopics() { return List.copyOf(recovered); }

	@Override
	public void createLog(TopicMetadata meta) {
		memory.createLog(meta);
		try {
			Path dir = topicsDir.resolve(meta.name());
			Files.createDirectories(dir);
			Files.writeString(dir.resolve("meta.json"), mapper.writeValueAsString(meta));
		} catch (IOException e) {
			memory.deleteLog(meta.name());
			throw new UncheckedIOException("failed to create log dir for topic '" + meta.name() + "'", e);
		}
	}

	@Override
	public void deleteLog(String topic) {
		// Close writers first so the delete succeeds and racing appends fail fast.
		writers.entrySet().removeIf(e -> {
			if (!e.getKey().topic().equals(topic)) return false;
			e.getValue().close();
			return true;
		});
		memory.deleteLog(topic);
		Path dir = topicsDir.resolve(topic);
		try (Stream<Path> walk = Files.walk(dir)) {
			walk.sorted(Comparator.reverseOrder()).forEach(p -> {
				try {
					Files.delete(p);
				} catch (IOException ex) {
					throw new UncheckedIOException(ex);
				}
			});
		} catch (NoSuchFileException ignored) {
			// already gone
		} catch (IOException e) {
			throw new UncheckedIOException("failed to delete log dir for topic '" + topic + "'", e);
		}
	}

	@Override
	public Message append(String topic, int partition, String key, Object value, Map<String, String> headers) {
		LogWriter writer = writers.computeIfAbsent(new TopicPartition(topic, partition),
				tp -> new LogWriter(partitionFile(tp.topic(), tp.partition())));
		// Lock spans offset assignment + file append so file order always matches offset order.
		synchronized (writer) {
			Message m = memory.append(topic, partition, key, value, headers);
			writer.writeLine(mapper.writeValueAsString(m));
			return m;
		}
	}

	@Override
	public List<Message> read(String topic, int partition, long fromOffset, int max) {
		return memory.read(topic, partition, fromOffset, max);
	}

	@Override
	public long endOffset(String topic, int partition) { return memory.endOffset(topic, partition); }

	@Override
	public long beginOffset(String topic, int partition) { return memory.beginOffset(topic, partition); }

	// Spring auto-invokes close() on context shutdown.
	public void close() {
		writers.values().forEach(LogWriter::close);
		writers.clear();
	}

	private Path partitionFile(String topic, int partition) {
		return topicsDir.resolve(topic).resolve(partition + ".log");
	}

	// One appender per partition file; callers synchronize on the instance.
	private static final class LogWriter {

		private final Path file;
		private BufferedWriter out;

		LogWriter(Path file) { this.file = file; }

		void writeLine(String line) {
			try {
				if (out == null) out = JsonLines.openAppend(file);
				out.write(line);
				out.write('\n');
				out.flush();
			} catch (IOException e) {
				throw new UncheckedIOException("append failed for " + file, e);
			}
		}

		void close() {
			try {
				if (out != null) out.close();
			} catch (IOException ignored) {
				// best-effort close on shutdown/delete
			}
		}
	}
}
