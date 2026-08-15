package com.example.streamix.client;

import java.util.Map;

public record ConsumedMessage(String topic, int partition, long offset, String key, Object value,
		Map<String, String> headers, long timestamp) {

	// Converts the JSON value into a typed object (record/POJO/Map).
	public <T> T valueAs(Class<T> type) {
		return Json.MAPPER.convertValue(value, type);
	}
}
