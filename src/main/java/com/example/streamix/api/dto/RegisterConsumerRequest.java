package com.example.streamix.api.dto;

import java.util.Set;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

// sessionTimeoutMs is optional; pick one longer than your polling interval.
public record RegisterConsumerRequest(@NotBlank String consumerId, @NotEmpty Set<String> topics, Long sessionTimeoutMs) {
}
