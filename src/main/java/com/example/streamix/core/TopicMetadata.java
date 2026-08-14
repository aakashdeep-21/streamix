package com.example.streamix.core;

// retentionMs/retentionBytes are per-topic overrides; null falls back to broker defaults.
public record TopicMetadata(String name, int partitions, long createdAt, Long retentionMs, Long retentionBytes) {

	public TopicMetadata(String name, int partitions, long createdAt) {
		this(name, partitions, createdAt, null, null);
	}
}
