package com.example.streamix.storage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.example.streamix.core.Message;
import com.example.streamix.core.Partition;
import com.example.streamix.core.TopicMetadata;

// Pure in-memory log; used standalone in tests and as the read path inside FileLogStorage.
public class InMemoryLogStorage implements LogStorage {

	private final Map<String, Partition[]> logs = new ConcurrentHashMap<>();

	@Override
	public void createLog(TopicMetadata meta) {
		Partition[] partitions = new Partition[meta.partitions()];
		for (int i = 0; i < partitions.length; i++) partitions[i] = new Partition();
		logs.put(meta.name(), partitions);
	}

	@Override
	public void deleteLog(String topic) { logs.remove(topic); }

	@Override
	public Message append(String topic, int partition, String key, Object value, Map<String, String> headers) {
		return partition(topic, partition).append(key, value, headers, System.currentTimeMillis());
	}

	@Override
	public List<Message> read(String topic, int partition, long fromOffset, int max) {
		return partition(topic, partition).read(fromOffset, max);
	}

	@Override
	public long endOffset(String topic, int partition) { return partition(topic, partition).endOffset(); }

	@Override
	public long beginOffset(String topic, int partition) { return partition(topic, partition).beginOffset(); }

	// Recovery hook used by FileLogStorage replay.
	void restore(String topic, int partition, Message m) { partition(topic, partition).restore(m); }

	private Partition partition(String topic, int partition) {
		Partition[] partitions = logs.get(topic);
		if (partitions == null) throw new IllegalStateException("no log for topic '" + topic + "'");
		if (partition < 0 || partition >= partitions.length) {
			throw new IllegalStateException("partition " + partition + " out of range for topic '" + topic + "'");
		}
		return partitions[partition];
	}
}
