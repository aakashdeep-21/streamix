package com.example.streamix.client;

// Transport-level failure (connect refused, timeout, interrupt) — the broker never saw the request outcome.
public class StreamixIoException extends StreamixException {

	public StreamixIoException(String message, Throwable cause) {
		super(message, cause);
	}
}
