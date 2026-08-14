package com.example.streamix.core;

import java.util.Map;

// Immutable stored record; value is arbitrary JSON, offset is partition-local and monotonic.
public record Message(long offset, String key, Object value, Map<String, String> headers, long timestamp) {
}
