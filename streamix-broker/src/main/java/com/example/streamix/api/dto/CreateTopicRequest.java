package com.example.streamix.api.dto;

import jakarta.validation.constraints.NotBlank;

// retentionMs/retentionBytes optionally override the broker-wide retention defaults.
public record CreateTopicRequest(@NotBlank String name, int partitions, Long retentionMs, Long retentionBytes) {
}
