package com.example.streamix.client;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// HTTP core: JSON in/out, broker errors → StreamixApiException, IO/5xx retried with backoff.
// Retries can duplicate a publish whose ack was lost — consistent with at-least-once delivery.
final class HttpJson {

	private static final Logger log = LoggerFactory.getLogger(HttpJson.class);

	private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
	private final String baseUrl;
	private final int maxRetries;
	private final long backoffMs;
	private final Duration defaultTimeout;

	HttpJson(String baseUrl, int maxRetries, long backoffMs, Duration defaultTimeout) {
		this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
		this.maxRetries = maxRetries;
		this.backoffMs = backoffMs;
		this.defaultTimeout = defaultTimeout;
	}

	<T> T get(String path, Class<T> type) { return get(path, type, defaultTimeout); }

	<T> T get(String path, Class<T> type, Duration timeout) {
		return exchange(HttpRequest.newBuilder(uri(path)).timeout(timeout).GET(), type);
	}

	<T> T post(String path, Object body, Class<T> type) {
		return exchange(HttpRequest.newBuilder(uri(path)).timeout(defaultTimeout)
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(Json.MAPPER.writeValueAsString(body))), type);
	}

	void delete(String path) {
		exchange(HttpRequest.newBuilder(uri(path)).timeout(defaultTimeout).DELETE(), Void.class);
	}

	static String seg(String pathSegment) {
		return URLEncoder.encode(pathSegment, StandardCharsets.UTF_8).replace("+", "%20");
	}

	private <T> T exchange(HttpRequest.Builder builder, Class<T> type) {
		HttpRequest request = builder.header("Accept", "application/json").build();
		for (int attempt = 0; ; attempt++) {
			String failure;
			try {
				HttpResponse<String> res = http.send(request, HttpResponse.BodyHandlers.ofString());
				if (res.statusCode() < 300) {
					if (type == Void.class || res.body() == null || res.body().isBlank()) return null;
					return Json.MAPPER.readValue(res.body(), type);
				}
				StreamixApiException apiError = toApiException(res);
				if (res.statusCode() < 500 || attempt >= maxRetries) throw apiError;
				failure = apiError.getMessage();
			} catch (IOException e) {
				if (attempt >= maxRetries) {
					throw new StreamixIoException("request failed: " + request.method() + " " + request.uri(), e);
				}
				failure = e.toString();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new StreamixIoException("interrupted: " + request.uri(), e);
			}
			log.warn("retrying {} {} (attempt {}/{}): {}", request.method(), request.uri(), attempt + 1, maxRetries, failure);
			sleep(backoffMs * (attempt + 1));
		}
	}

	private StreamixApiException toApiException(HttpResponse<String> res) {
		try {
			ApiErrorBody body = Json.MAPPER.readValue(res.body(), ApiErrorBody.class);
			return new StreamixApiException(body.status(), body.error(), body.message());
		} catch (RuntimeException notOurBody) {
			return new StreamixApiException(res.statusCode(), "HTTP_" + res.statusCode(), truncate(res.body()));
		}
	}

	private URI uri(String path) { return URI.create(baseUrl + path); }

	private static String truncate(String s) {
		if (s == null) return "";
		return s.length() > 200 ? s.substring(0, 200) : s;
	}

	private static void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new StreamixIoException("interrupted during retry backoff", e);
		}
	}
}
