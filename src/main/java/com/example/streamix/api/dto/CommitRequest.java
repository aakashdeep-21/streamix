package com.example.streamix.api.dto;

import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

public record CommitRequest(@NotEmpty List<@Valid CommitEntryDto> offsets) {
}
