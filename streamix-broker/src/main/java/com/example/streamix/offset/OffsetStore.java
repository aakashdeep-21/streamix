package com.example.streamix.offset;

import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.streamix.core.TopicPartition;

import jakarta.annotation.PostConstruct;

// Committed offsets per (group, topic, partition); commit offset = next offset to read.
@Component
public class OffsetStore {

	private final ConcurrentHashMap<GroupTopicPartition, Long> committed = new ConcurrentHashMap<>();
	private final OffsetJournal journal;

	public OffsetStore(OffsetJournal journal) {
		this.journal = journal;
	}

	@PostConstruct
	void replayJournal() {
		journal.replay(committed::put);
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

	// Journal rows for deleted topics persist until Phase 2 snapshotting; read views filter them out.
	public void purgeTopic(String topic) {
		committed.keySet().removeIf(k -> k.topic().equals(topic));
	}
}
