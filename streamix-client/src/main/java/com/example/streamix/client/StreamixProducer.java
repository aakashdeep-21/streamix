package com.example.streamix.client;

import java.time.Duration;
import java.util.List;
import java.util.Map;

// Publishes to Streamix over REST; acks carry the assigned partition + offset.
public final class StreamixProducer {

	private final HttpJson http;

	private StreamixProducer(HttpJson http) {
		this.http = http;
	}

	public static StreamixProducer create(String baseUrl) { return builder(baseUrl).build(); }

	public static Builder builder(String baseUrl) { return new Builder(baseUrl); }

	public PublishAck send(String topic, Object value) { return send(topic, null, value, null); }

	public PublishAck send(String topic, String key, Object value) { return send(topic, key, value, null); }

	public PublishAck send(String topic, String key, Object value, Map<String, String> headers) {
		return http.post("/api/v1/topics/" + HttpJson.seg(topic) + "/messages",
				new ProducerMessage(key, value, headers), PublishAck.class);
	}

	// Non-transactional: the broker acks each message individually.
	public List<PublishAck> sendBatch(String topic, List<ProducerMessage> messages) {
		BatchBody res = http.post("/api/v1/topics/" + HttpJson.seg(topic) + "/messages/batch",
				Map.of("messages", messages), BatchBody.class);
		return res.acks();
	}

	public record BatchBody(int count, List<PublishAck> acks) {}

	public static final class Builder {

		private final String baseUrl;
		private int maxRetries = 3;
		private long backoffMs = 200;
		private long requestTimeoutMs = 15_000;

		private Builder(String baseUrl) { this.baseUrl = baseUrl; }

		public Builder maxRetries(int v) { this.maxRetries = v; return this; }

		public Builder backoffMs(long v) { this.backoffMs = v; return this; }

		public Builder requestTimeoutMs(long v) { this.requestTimeoutMs = v; return this; }

		public StreamixProducer build() {
			return new StreamixProducer(new HttpJson(baseUrl, maxRetries, backoffMs, Duration.ofMillis(requestTimeoutMs)));
		}
	}
}
