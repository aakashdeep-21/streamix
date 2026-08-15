package com.example.streamix.group;

import java.util.List;

import com.example.streamix.core.TopicPartition;

public record RegistrationResult(long sessionTimeoutMs, List<TopicPartition> assignedPartitions) {
}
