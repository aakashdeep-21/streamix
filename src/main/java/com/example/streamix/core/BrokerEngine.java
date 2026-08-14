package com.example.streamix.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.example.streamix.config.BrokerProperties;
import com.example.streamix.group.GroupCoordinator;
import com.example.streamix.group.OffsetCommit;
import com.example.streamix.group.PolledMessage;
import com.example.streamix.group.RegistrationResult;
import com.example.streamix.offset.OffsetStore;
import com.example.streamix.storage.LogStorage;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// Single entry point for the API layer; orchestrates topics, storage, groups and offsets.
@Service
public class BrokerEngine {

	private final TopicManager topicManager;
	private final Partitioner partitioner;
	private final LogStorage storage;
	private final GroupCoordinator coordinator;
	private final OffsetStore offsetStore;
	private final BrokerProperties props;
	private final BrokerMetrics metrics;
	private final ObjectMapper sizeMapper = JsonMapper.builder().build();

	public BrokerEngine(TopicManager topicManager, Partitioner partitioner, LogStorage storage,
			GroupCoordinator coordinator, OffsetStore offsetStore, BrokerProperties props, BrokerMetrics metrics) {
		this.topicManager = topicManager;
		this.partitioner = partitioner;
		this.storage = storage;
		this.coordinator = coordinator;
		this.offsetStore = offsetStore;
		this.props = props;
		this.metrics = metrics;
	}

	// --- topics ---

	public TopicMetadata createTopic(String name, int partitions, Long retentionMs, Long retentionBytes) {
		return topicManager.create(name, partitions, retentionMs, retentionBytes);
	}

	public List<TopicMetadata> listTopics() { return topicManager.list(); }

	public TopicMetadata topic(String name) { return topicManager.get(name); }

	public List<PartitionOffsets> topicOffsets(String name) {
		TopicMetadata meta = topicManager.get(name);
		List<PartitionOffsets> out = new ArrayList<>(meta.partitions());
		for (int p = 0; p < meta.partitions(); p++) {
			out.add(new PartitionOffsets(p, storage.beginOffset(name, p), storage.endOffset(name, p)));
		}
		return out;
	}

	// Order matters: consumers are unassigned before the log disappears.
	public void deleteTopic(String name) {
		topicManager.delete(name);
		coordinator.onTopicDeleted(name);
		offsetStore.purgeTopic(name);
		partitioner.forget(name);
		storage.deleteLog(name);
	}

	// --- publish ---

	public PublishResult publish(String topic, String key, Object value, Map<String, String> headers) {
		TopicMetadata meta = topicManager.get(topic);
		enforceSize(value);
		int partition = partitioner.partition(topic, key, meta.partitions());
		Message m = storage.append(topic, partition, key, value, headers);
		metrics.published(1);
		return new PublishResult(topic, partition, m.offset(), m.timestamp());
	}

	// Non-transactional: each message is appended and acked individually.
	public List<PublishResult> publishBatch(String topic, List<ProducerRecord> records) {
		if (records.size() > props.getBatchMaxSize()) {
			throw BrokerException.invalidArgument("batch of " + records.size() + " exceeds max " + props.getBatchMaxSize());
		}
		topicManager.get(topic); // fail fast before appending anything
		return records.stream().map(r -> publish(topic, r.key(), r.value(), r.headers())).toList();
	}

	// --- consume ---

	public RegistrationResult register(String groupId, String consumerId, Set<String> topics, Long sessionTimeoutMs) {
		return coordinator.register(groupId, consumerId, topics, sessionTimeoutMs);
	}

	public void deregister(String groupId, String consumerId) {
		coordinator.deregister(groupId, consumerId);
	}

	public List<PolledMessage> poll(String groupId, String consumerId, Integer requestedMax) {
		int max = requestedMax == null ? props.getPollDefaultMax() : requestedMax;
		if (max < 1) throw BrokerException.invalidArgument("max must be >= 1");
		List<PolledMessage> polled = coordinator.poll(groupId, consumerId, Math.min(max, props.getPollMaxLimit()));
		metrics.consumed(polled.size());
		return polled;
	}

	public void commit(String groupId, String consumerId, List<OffsetCommit> entries) {
		coordinator.commit(groupId, consumerId, entries);
		metrics.committed(entries.size());
	}

	public List<GroupOffsetView> groupOffsets(String groupId, String topicFilter) {
		List<GroupOffsetView> out = new ArrayList<>();
		offsetStore.snapshot(groupId, topicFilter).forEach((tp, committed) -> {
			if (topicManager.find(tp.topic()) == null) return; // ghost rows from deleted topics
			long end = storage.endOffset(tp.topic(), tp.partition());
			// lag counts only still-retained messages: trimmed ones can never be consumed
			long effective = Math.max(committed, storage.beginOffset(tp.topic(), tp.partition()));
			out.add(new GroupOffsetView(tp.topic(), tp.partition(), committed, end, end - effective));
		});
		return out;
	}

	private void enforceSize(Object value) {
		byte[] bytes = sizeMapper.writeValueAsBytes(value);
		if (bytes.length > props.getMaxMessageBytes()) {
			throw BrokerException.messageTooLarge(bytes.length, props.getMaxMessageBytes());
		}
	}
}
