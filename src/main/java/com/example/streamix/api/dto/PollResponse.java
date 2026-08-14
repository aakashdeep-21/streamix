package com.example.streamix.api.dto;

import java.util.List;

public record PollResponse(int count, List<PolledMessageDto> messages) {
}
