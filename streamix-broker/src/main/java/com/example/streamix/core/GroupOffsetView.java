package com.example.streamix.core;

// Ops view: committed progress + lag per partition for a group.
public record GroupOffsetView(String topic, int partition, long committedOffset, long endOffset, long lag) {
}
