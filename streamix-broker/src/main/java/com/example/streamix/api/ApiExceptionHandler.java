package com.example.streamix.api;

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.ErrorResponse;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import com.example.streamix.api.dto.ApiError;
import com.example.streamix.core.BrokerException;

import jakarta.servlet.http.HttpServletRequest;

// Maps every failure to the uniform ApiError body; status comes from the domain ErrorCode.
@RestControllerAdvice
public class ApiExceptionHandler {

	private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

	@ExceptionHandler(BrokerException.class)
	ResponseEntity<ApiError> broker(BrokerException ex, HttpServletRequest req) {
		return build(ex.code().status(), ex.code().name(), ex.getMessage(), req);
	}

	@ExceptionHandler(MethodArgumentNotValidException.class)
	ResponseEntity<ApiError> validation(MethodArgumentNotValidException ex, HttpServletRequest req) {
		String message = ex.getBindingResult().getFieldErrors().stream()
				.map(f -> f.getField() + " " + f.getDefaultMessage())
				.sorted()
				.collect(Collectors.joining("; "));
		return build(400, "VALIDATION_FAILED", message, req);
	}

	@ExceptionHandler(HttpMessageNotReadableException.class)
	ResponseEntity<ApiError> unreadable(HttpMessageNotReadableException ex, HttpServletRequest req) {
		return build(400, "MALFORMED_REQUEST", "request body is not valid JSON for this endpoint", req);
	}

	@ExceptionHandler(MethodArgumentTypeMismatchException.class)
	ResponseEntity<ApiError> typeMismatch(MethodArgumentTypeMismatchException ex, HttpServletRequest req) {
		return build(400, "INVALID_ARGUMENT", "parameter '" + ex.getName() + "' has an invalid value", req);
	}

	@ExceptionHandler(Exception.class)
	ResponseEntity<ApiError> fallback(Exception ex, HttpServletRequest req) {
		// Spring's own 404/405/etc keep their status but get our body shape.
		if (ex instanceof ErrorResponse er) {
			return build(er.getStatusCode().value(), "REQUEST_ERROR", ex.getMessage(), req);
		}
		log.error("unhandled error on {} {}", req.getMethod(), req.getRequestURI(), ex);
		return build(500, "INTERNAL_ERROR", "unexpected broker error", req);
	}

	private ResponseEntity<ApiError> build(int status, String error, String message, HttpServletRequest req) {
		return ResponseEntity.status(status)
				.body(new ApiError(System.currentTimeMillis(), status, error, message, req.getRequestURI()));
	}
}
