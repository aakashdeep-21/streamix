package com.example.streamix.core;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.example.streamix.config.BrokerProperties;
import com.example.streamix.storage.LogStorage;

import jakarta.annotation.PostConstruct;

@Component
public class TopicManager {

	private static final Logger log = LoggerFactory.getLogger(TopicManager.class);
	private static final Pattern NAME = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9._-]{0,254}$");

	private final ConcurrentHashMap<String, TopicMetadata> topics = new ConcurrentHashMap<>();
	private final LogStorage storage;
	private final BrokerProperties props;

	public TopicManager(LogStorage storage, BrokerProperties props) {
		this.storage = storage;
		this.props = props;
	}

	// Storage recovery runs before this bean initializes (it depends on the storage bean).
	@PostConstruct
	void registerRecovered() {
		storage.recoveredTopics().forEach(meta -> topics.put(meta.name(), meta));
	}

	public TopicMetadata create(String name, int partitions, Long retentionMs, Long retentionBytes) {
		if (name == null || !NAME.matcher(name).matches()) {
			throw BrokerException.invalidArgument("topic name must match [a-zA-Z0-9][a-zA-Z0-9._-]* (max 255 chars)");
		}
		if (partitions < 1 || partitions > props.getMaxPartitionsPerTopic()) {
			throw BrokerException.invalidArgument("partitions must be between 1 and " + props.getMaxPartitionsPerTopic());
		}
		if ((retentionMs != null && retentionMs < 1) || (retentionBytes != null && retentionBytes < 1)) {
			throw BrokerException.invalidArgument("retentionMs and retentionBytes overrides must be positive when set");
		}
		TopicMetadata meta = new TopicMetadata(name, partitions, System.currentTimeMillis(), retentionMs, retentionBytes);
		if (topics.putIfAbsent(name, meta) != null) throw BrokerException.duplicateTopic(name);
		try {
			storage.createLog(meta);
		} catch (RuntimeException e) {
			topics.remove(name);
			throw e;
		}
		log.info("created topic '{}': {} partitions, retentionMs={}, retentionBytes={}",
				name, partitions, retentionMs, retentionBytes);
		return meta;
	}

	public TopicMetadata get(String name) {
		TopicMetadata meta = topics.get(name);
		if (meta == null) throw BrokerException.unknownTopic(name);
		return meta;
	}

	public TopicMetadata find(String name) { return topics.get(name); }

	public List<TopicMetadata> list() {
		return topics.values().stream().sorted(Comparator.comparing(TopicMetadata::name)).toList();
	}

	// Registry removal only; BrokerEngine orchestrates coordinator/offset/log cleanup around it.
	public void delete(String name) {
		if (topics.remove(name) == null) throw BrokerException.unknownTopic(name);
	}

	public int count() { return topics.size(); }
}
