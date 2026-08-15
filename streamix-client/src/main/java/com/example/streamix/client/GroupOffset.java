package com.example.streamix.client;

// Committed progress + lag for one partition of a group (ops/monitoring view).
public record GroupOffset(String topic, int partition, long committedOffset, long endOffset, long lag) {
}
