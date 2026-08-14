package com.example.streamix.api.dto;

import java.util.Map;

import com.example.streamix.group.PolledMessage;

public record PolledMessageDto(String topic, int partition, long offset, String key, Object value,
		Map<String, String> headers, long timestamp) {

	public static PolledMessageDto from(PolledMessage p) {
		return new PolledMessageDto(p.topic(), p.partition(), p.message().offset(), p.message().key(),
				p.message().value(), p.message().headers(), p.message().timestamp());
	}
}
