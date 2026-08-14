package com.example.streamix.api.dto;

import jakarta.validation.constraints.NotBlank;

public record CreateTopicRequest(@NotBlank String name, int partitions) {
}
