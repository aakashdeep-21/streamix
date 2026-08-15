package com.example.streamix.client;

// Mirrors the broker's uniform error body.
record ApiErrorBody(long timestamp, int status, String error, String message, String path) {
}
