package com.example.streamix.core;

// Machine-readable error codes; the API layer maps status() to the HTTP response.
public enum ErrorCode {
	UNKNOWN_TOPIC(404),
	UNKNOWN_CONSUMER(404),
	DUPLICATE_TOPIC(409),
	DUPLICATE_CONSUMER(409),
	PARTITION_NOT_ASSIGNED(409),
	INVALID_OFFSET(400),
	INVALID_ARGUMENT(400),
	MESSAGE_TOO_LARGE(413);

	private final int status;

	ErrorCode(int status) { this.status = status; }

	public int status() { return status; }
}
