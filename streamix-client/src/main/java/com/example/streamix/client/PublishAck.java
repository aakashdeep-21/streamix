package com.example.streamix.client;

public record PublishAck(String topic, int partition, long offset, long timestamp) {
}
