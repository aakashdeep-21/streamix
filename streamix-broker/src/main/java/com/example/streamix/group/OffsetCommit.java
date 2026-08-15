package com.example.streamix.group;

// A single commit entry; offset is the next offset the consumer wants to read.
public record OffsetCommit(String topic, int partition, long offset) {
}
