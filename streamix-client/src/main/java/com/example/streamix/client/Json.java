package com.example.streamix.client;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// Shared mapper; unknown fields are ignored so newer brokers stay compatible with older clients.
final class Json {

	static final ObjectMapper MAPPER = JsonMapper.builder()
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.build();

	private Json() {}
}
