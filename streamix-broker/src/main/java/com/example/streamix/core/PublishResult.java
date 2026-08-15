package com.example.streamix.core;

// Producer ack: where the message landed.
public record PublishResult(String topic, int partition, long offset, long timestamp) {
}
