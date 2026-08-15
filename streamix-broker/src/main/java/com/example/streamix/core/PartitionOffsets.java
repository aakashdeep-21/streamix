package com.example.streamix.core;

public record PartitionOffsets(int partition, long beginOffset, long endOffset) {
}
