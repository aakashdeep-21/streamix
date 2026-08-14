package com.example.streamix.api.dto;

import jakarta.validation.constraints.NotBlank;

// offset = next offset to read (Kafka convention), not the last processed one.
public record CommitEntryDto(@NotBlank String topic, int partition, long offset) {
}
