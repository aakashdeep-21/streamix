package com.example.streamix.core;

import java.util.Map;

// An incoming message before the broker assigns partition + offset.
public record ProducerRecord(String key, Object value, Map<String, String> headers) {
}
