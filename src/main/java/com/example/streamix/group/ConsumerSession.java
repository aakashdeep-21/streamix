package com.example.streamix.group;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.example.streamix.core.TopicPartition;

// Mutable session state; mutations happen under the owning group's monitor (heartbeat is volatile).
final class ConsumerSession {

	final String consumerId;
	final Set<String> topics;
	final long sessionTimeoutMs;
	volatile long lastSeenMs;
	final Map<TopicPartition, Long> positions = new HashMap<>();
	long pollRotation = 0;

	ConsumerSession(String consumerId, Set<String> topics, long sessionTimeoutMs, long now) {
		this.consumerId = consumerId;
		this.topics = new HashSet<>(topics);
		this.sessionTimeoutMs = sessionTimeoutMs;
		this.lastSeenMs = now;
	}
}
