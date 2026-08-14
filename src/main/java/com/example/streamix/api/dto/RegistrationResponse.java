package com.example.streamix.api.dto;

import java.util.List;

import com.example.streamix.core.TopicPartition;

public record RegistrationResponse(String group, String consumerId, long sessionTimeoutMs,
		List<TopicPartition> assignedPartitions) {
}
