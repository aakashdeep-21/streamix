package com.example.streamix.client;

// A non-2xx broker response; code mirrors the broker's machine-readable error codes.
public class StreamixApiException extends StreamixException {

	private final int status;
	private final String code;

	public StreamixApiException(int status, String code, String message) {
		super(code + " (" + status + "): " + message);
		this.status = status;
		this.code = code;
	}

	public int status() { return status; }

	public String code() { return code; }

	public boolean is(String errorCode) { return code.equals(errorCode); }
}
