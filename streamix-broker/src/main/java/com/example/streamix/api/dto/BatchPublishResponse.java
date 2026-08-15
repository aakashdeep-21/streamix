package com.example.streamix.api.dto;

import java.util.List;

import com.example.streamix.core.PublishResult;

public record BatchPublishResponse(int count, List<PublishResult> acks) {
}
