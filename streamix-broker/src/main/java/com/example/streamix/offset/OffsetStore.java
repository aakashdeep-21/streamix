package com.example.streamix.offset;

import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.streamix.core.TopicPartition;

import jakarta.annotation.PostConstruct;

// Committed offsets per (group, topic, partition); commit offset = next offset to read.
@Component
public class OffsetStore {

	private static final Logger log = LoggerFactory.getLogger(OffsetStore.class);

	private final ConcurrentHashMap<GroupTopicPartition, Long> committed = new ConcurrentHashMap<>();
	private final OffsetJournal journal;

	public OffsetStore(OffsetJournal journal) {
		this.journal = journal;
	}

	@PostConstruct
	void replayJournal() {
		journal.replay(committed::put);
		if (!committed.isEmpty()) log.info("restored {} committed offset(s) from disk", committed.size());
	}

	// Synchronized with snapshotTick so no commit can slip between map copy and journal truncation.
	public synchronized void commit(String group, String topic, int partition, long offset) {
		GroupTopicPartition key = new GroupTopicPartition(group, topic, partition);
		committed.put(key, offset);
		journal.append(key, offset);
	}

	@Scheduled(fixedDelayString = "${streamix.offset-snapshot-ms:300000}")
	public synchronized void snapshotTick() {
		journal.snapshot(Map.copyOf(committed));
	}

	public OptionalLong fetch(String group, String topic, int partition) {
		Long v = committed.get(new GroupTopicPartition(group, topic, partition));
		return v == null ? OptionalLong.empty() : OptionalLong.of(v);
	}

	// Committed offsets for a group, optionally filtered by topic.
	public Map<TopicPartition, Long> snapshot(String group, String topicFilter) {
		Map<TopicPartition, Long> out = new TreeMap<>();
		committed.forEach((k, v) -> {
			if (k.group().equals(group) && (topicFilter == null || k.topic().equals(topicFilter))) {
				out.put(new TopicPartition(k.topic(), k.partition()), v);
			}
		});
		return out;
	}

	// Journal rows for deleted topics persist until the next snapshot; read views filter them out.
	public void purgeTopic(String topic) {
		int before = committed.size();
		committed.keySet().removeIf(k -> k.topic().equals(topic));
		int removed = before - committed.size();
		if (removed > 0) log.debug("purged {} committed offset(s) for deleted topic '{}'", removed, topic);
	}
}
