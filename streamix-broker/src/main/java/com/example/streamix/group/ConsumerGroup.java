package com.example.streamix.group;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.example.streamix.core.TopicPartition;

// Membership + assignment guarded by this object's monitor; epoch invalidates in-flight polls.
final class ConsumerGroup {

	final String groupId;
	final Map<String, ConsumerSession> members = new ConcurrentHashMap<>();
	final Map<TopicPartition, String> assignment = new HashMap<>();
	long epoch = 0;

	ConsumerGroup(String groupId) {
		this.groupId = groupId;
	}
}
