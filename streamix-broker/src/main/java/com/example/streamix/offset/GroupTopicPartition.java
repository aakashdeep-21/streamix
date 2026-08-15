package com.example.streamix.offset;

public record GroupTopicPartition(String group, String topic, int partition) {
}
