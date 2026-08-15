package com.example.streamix.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.example.streamix.config.BrokerProperties;
import com.example.streamix.core.Message;
import com.example.streamix.core.TopicMetadata;
import com.example.streamix.core.TopicPartition;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// Durable log: rolling JSON-lines segments per partition, reads served from disk via sparse indexes.
public class FileLogStorage implements LogStorage {

	private static final Logger log = LoggerFactory.getLogger(FileLogStorage.class);

	enum FsyncMode { ALWAYS, INTERVAL, NEVER }

	private final ObjectMapper mapper = JsonMapper.builder().build();
	private final Path topicsDir;
	private final long segmentMaxBytes;
	private final FsyncMode fsyncMode;
	private final ConcurrentHashMap<TopicPartition, SegmentedPartitionLog> logs = new ConcurrentHashMap<>();
	private final List<TopicMetadata> recovered = new ArrayList<>();

	public FileLogStorage(BrokerProperties props) {
		this.topicsDir = Path.of(props.getDataDir()).resolve("topics");
		this.segmentMaxBytes = props.getSegmentMaxBytes();
		try {
			this.fsyncMode = FsyncMode.valueOf(props.getFsync().trim().toUpperCase());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("streamix.fsync must be one of: always, interval, never");
		}
	}

	// Startup-only: rebuild all partition logs from disk before the broker serves traffic.
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
		for (int p = 0; p < meta.partitions(); p++) {
			migrateV1Layout(dir, p);
			SegmentedPartitionLog partitionLog = newPartitionLog(meta.name(), p);
			partitionLog.recover();
			logs.put(new TopicPartition(meta.name(), p), partitionLog);
		}
		recovered.add(meta);
		log.info("recovered topic '{}' ({} partitions)", meta.name(), meta.partitions());
	}

	// Phase 1 stored a single <p>.log per partition; it becomes segment 0 of the segmented layout.
	private void migrateV1Layout(Path topicDir, int partition) throws IOException {
		Path old = topicDir.resolve(partition + ".log");
		if (!Files.isRegularFile(old)) return;
		Path segmentDir = topicDir.resolve(String.valueOf(partition));
		Files.createDirectories(segmentDir);
		Files.move(old, segmentDir.resolve(String.format("%020d.log", 0L)));
		log.info("migrated v1 log {} into segmented layout", old);
	}

	@Override
	public List<TopicMetadata> recoveredTopics() { return List.copyOf(recovered); }

	@Override
	public void createLog(TopicMetadata meta) {
		try {
			Path dir = topicsDir.resolve(meta.name());
			Files.createDirectories(dir);
			Files.writeString(dir.resolve("meta.json"), mapper.writeValueAsString(meta));
			for (int p = 0; p < meta.partitions(); p++) {
				SegmentedPartitionLog partitionLog = newPartitionLog(meta.name(), p);
				partitionLog.recover(); // empty dir → zeroed state
				logs.put(new TopicPartition(meta.name(), p), partitionLog);
			}
		} catch (IOException e) {
			deleteLog(meta.name());
			throw new UncheckedIOException("failed to create log dirs for topic '" + meta.name() + "'", e);
		}
	}

	@Override
	public void deleteLog(String topic) {
		logs.entrySet().removeIf(e -> {
			if (!e.getKey().topic().equals(topic)) return false;
			e.getValue().close();
			return true;
		});
		Path dir = topicsDir.resolve(topic);
		try (Stream<Path> walk = Files.walk(dir)) {
			walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
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
		return partitionLog(topic, partition).append(key, value, headers);
	}

	@Override
	public List<Message> read(String topic, int partition, long fromOffset, int max) {
		return partitionLog(topic, partition).read(fromOffset, max);
	}

	@Override
	public long endOffset(String topic, int partition) { return partitionLog(topic, partition).endOffset(); }

	@Override
	public long beginOffset(String topic, int partition) { return partitionLog(topic, partition).beginOffset(); }

	@Override
	public long enforceRetention(String topic, int partition, long minTimestampMs, long maxBytes) {
		return partitionLog(topic, partition).enforceRetention(minTimestampMs, maxBytes);
	}

	@Override
	public long totalRetainedBytes() {
		return logs.values().stream().mapToLong(SegmentedPartitionLog::sizeBytes).sum();
	}

	@Override
	public int totalSegments() {
		return logs.values().stream().mapToInt(SegmentedPartitionLog::segmentCount).sum();
	}

	// ALWAYS syncs inline per append; NEVER leaves flushing to the OS entirely.
	@Scheduled(fixedDelayString = "${streamix.fsync-interval-ms:1000}")
	public void fsyncTick() {
		if (fsyncMode != FsyncMode.INTERVAL) return;
		logs.values().forEach(SegmentedPartitionLog::sync);
	}

	// Spring auto-invokes close() on context shutdown.
	public void close() {
		logs.values().forEach(SegmentedPartitionLog::close);
		logs.clear();
	}

	private SegmentedPartitionLog partitionLog(String topic, int partition) {
		SegmentedPartitionLog partitionLog = logs.get(new TopicPartition(topic, partition));
		if (partitionLog == null) throw new IllegalStateException("no log for topic '" + topic + "' partition " + partition);
		return partitionLog;
	}

	private SegmentedPartitionLog newPartitionLog(String topic, int partition) {
		return new SegmentedPartitionLog(topicsDir.resolve(topic).resolve(String.valueOf(partition)),
				mapper, segmentMaxBytes, fsyncMode == FsyncMode.ALWAYS);
	}
}
