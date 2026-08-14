package com.example.streamix.api.dto;

// Uniform error body for every non-2xx response.
public record ApiError(long timestamp, int status, String error, String message, String path) {
}
