package com.example.streamix.storage;

import java.util.List;
import java.util.Map;

import com.example.streamix.core.Message;
import com.example.streamix.core.TopicMetadata;

// Log abstraction: v1 serves reads from memory; the file impl adds write-through durability.
public interface LogStorage {

	void createLog(TopicMetadata meta);

	void deleteLog(String topic);

	Message append(String topic, int partition, String key, Object value, Map<String, String> headers);

	List<Message> read(String topic, int partition, long fromOffset, int max);

	long endOffset(String topic, int partition);

	long beginOffset(String topic, int partition);

	// Topics found on disk at startup; empty for non-durable impls.
	default List<TopicMetadata> recoveredTopics() { return List.of(); }

	// Drops retained data older than minTimestampMs and beyond maxBytes (<=0 = unlimited); returns removed count.
	long enforceRetention(String topic, int partition, long minTimestampMs, long maxBytes);

	default long totalRetainedBytes() { return 0; }

	default int totalSegments() { return 0; }
}
