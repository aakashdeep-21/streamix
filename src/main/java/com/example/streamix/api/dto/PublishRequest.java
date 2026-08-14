package com.example.streamix.api.dto;

import java.util.Map;

import jakarta.validation.constraints.NotNull;

// value is arbitrary JSON (object, array, string, number); key routes to a partition when present.
public record PublishRequest(String key, @NotNull Object value, Map<String, String> headers) {
}
