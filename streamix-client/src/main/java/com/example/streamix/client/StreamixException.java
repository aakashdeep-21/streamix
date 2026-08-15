package com.example.streamix.client;

public class StreamixException extends RuntimeException {

	public StreamixException(String message) {
		super(message);
	}

	public StreamixException(String message, Throwable cause) {
		super(message, cause);
	}
}
