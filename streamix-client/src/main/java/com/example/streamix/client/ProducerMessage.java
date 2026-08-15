package com.example.streamix.client;

import java.util.Map;

// value is arbitrary JSON-serializable data; key (optional) pins per-key partition ordering.
public record ProducerMessage(String key, Object value, Map<String, String> headers) {

	public static ProducerMessage of(Object value) { return new ProducerMessage(null, value, null); }

	public static ProducerMessage of(String key, Object value) { return new ProducerMessage(key, value, null); }
}
