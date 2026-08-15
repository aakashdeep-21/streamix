package com.example.streamix.client;

public record TopicInfo(String name, int partitions, long createdAt, Long retentionMs, Long retentionBytes) {
}
