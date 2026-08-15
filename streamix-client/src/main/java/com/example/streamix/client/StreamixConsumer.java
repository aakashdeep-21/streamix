package com.example.streamix.client;

import java.net.InetAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Group consumer: runs the poll loop, keeps the session alive, commits after successful handling,
// re-registers on eviction, and resets to the last commit when a handler fails (redelivery).
public final class StreamixConsumer implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(StreamixConsumer.class);

	private final HttpJson http;
	private final String group;
	private final String consumerId;
	private final Set<String> topics;
	private final Long sessionTimeoutMs;
	private final int pollMax;
	private final long waitMs;
	private final boolean autoCommit;
	private final boolean takeoverOnDuplicate;
	private final long retryBackoffMs;

	private final AtomicBoolean running = new AtomicBoolean(false);
	private Thread pollThread;

	private StreamixConsumer(Builder b) {
		this.http = new HttpJson(b.baseUrl, b.maxRetries, b.backoffMs, Duration.ofMillis(b.requestTimeoutMs));
		this.group = b.group;
		this.consumerId = b.consumerId != null ? b.consumerId : defaultConsumerId();
		this.topics = Set.copyOf(b.topics);
		this.sessionTimeoutMs = b.sessionTimeoutMs;
		this.pollMax = b.pollMax;
		this.waitMs = b.waitMs;
		this.autoCommit = b.autoCommit;
		this.takeoverOnDuplicate = b.takeoverOnDuplicate;
		this.retryBackoffMs = b.retryBackoffMs;
	}

	public static Builder builder(String baseUrl) { return new Builder(baseUrl); }

	public String consumerId() { return consumerId; }

	// --- continuous mode ---

	public void start(MessageHandler handler) {
		if (!running.compareAndSet(false, true)) throw new IllegalStateException("consumer already running");
		log.info("starting consumer '{}' in group '{}' (topics={}, waitMs={}, autoCommit={})",
				consumerId, group, topics, waitMs, autoCommit);
		register();
		pollThread = new Thread(() -> runLoop(handler), "streamix-consumer-" + group + "-" + consumerId);
		pollThread.setDaemon(true);
		pollThread.start();
	}

	private void runLoop(MessageHandler handler) {
		while (running.get()) {
			List<ConsumedMessage> batch;
			try {
				batch = poll();
			} catch (StreamixApiException e) {
				if (e.is("UNKNOWN_CONSUMER")) {
					log.info("session for '{}' expired; re-registering", consumerId);
					registerUntilRunning();
				} else {
					log.warn("poll failed: {}", e.getMessage());
					sleepQuietly(retryBackoffMs);
				}
				continue;
			} catch (StreamixIoException e) {
				if (running.get()) log.warn("broker unreachable: {}", e.getMessage());
				sleepQuietly(retryBackoffMs);
				continue;
			}
			if (batch.isEmpty()) continue; // long poll already waited server-side
			log.debug("received {} message(s)", batch.size());
			try {
				handler.handle(batch);
			} catch (Exception handlerError) {
				log.error("handler failed; resetting to last commit for redelivery", handlerError);
				tryDeregister();
				registerUntilRunning();
				sleepQuietly(retryBackoffMs);
				continue;
			}
			if (autoCommit) {
				try {
					commitBatch(batch);
				} catch (StreamixException e) {
					log.warn("commit failed ({}); batch may be redelivered", e.getMessage());
				}
			}
		}
		tryDeregister();
	}

	// --- batch mode: one-shot drain for cron-style consumers ---

	public long drainAll(MessageHandler handler) {
		if (running.get()) throw new IllegalStateException("consumer is running in continuous mode");
		long start = System.currentTimeMillis();
		register();
		long total = 0;
		try {
			while (true) {
				List<ConsumedMessage> batch = pollOnce(0);
				if (batch.isEmpty()) {
					log.info("drained {} message(s) for group '{}' in {}ms", total, group, System.currentTimeMillis() - start);
					return total;
				}
				try {
					handler.handle(batch);
				} catch (Exception e) {
					throw new StreamixException("handler failed after " + total + " messages; uncommitted batch will be redelivered", e);
				}
				commitBatch(batch);
				total += batch.size();
			}
		} finally {
			tryDeregister();
		}
	}

	// --- building blocks (also usable directly for manual control) ---

	public List<ConsumedMessage> poll() { return pollOnce(waitMs); }

	public void commitBatch(List<ConsumedMessage> processed) {
		Map<String, long[]> next = new HashMap<>(); // "topic|partition" → next offset to read
		Map<String, ConsumedMessage> samples = new HashMap<>();
		for (ConsumedMessage m : processed) {
			String k = m.topic() + "|" + m.partition();
			long[] cur = next.computeIfAbsent(k, x -> new long[] {Long.MIN_VALUE});
			cur[0] = Math.max(cur[0], m.offset() + 1);
			samples.put(k, m);
		}
		List<Map<String, Object>> offsets = next.entrySet().stream().map(e -> {
			ConsumedMessage sample = samples.get(e.getKey());
			return Map.<String, Object>of("topic", sample.topic(), "partition", sample.partition(), "offset", e.getValue()[0]);
		}).toList();
		commitRaw(offsets);
	}

	public void commit(String topic, int partition, long nextOffset) {
		commitRaw(List.of(Map.of("topic", topic, "partition", partition, "offset", nextOffset)));
	}

	@Override
	public void close() {
		if (running.compareAndSet(true, false)) {
			if (pollThread != null) {
				pollThread.interrupt();
				try {
					pollThread.join(5000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			log.info("consumer '{}' in group '{}' stopped", consumerId, group);
		}
	}

	// --- internals ---

	private List<ConsumedMessage> pollOnce(long wait) {
		String path = consumerPath() + "/messages?max=" + pollMax + "&waitMs=" + wait;
		PollBody body = http.get(path, PollBody.class, Duration.ofMillis(wait + 10_000));
		return body == null || body.messages() == null ? List.of() : body.messages();
	}

	private void commitRaw(List<Map<String, Object>> offsets) {
		if (offsets.isEmpty()) return;
		http.post(consumerPath() + "/offsets", Map.of("offsets", offsets), Void.class);
		log.debug("committed {}", offsets);
	}

	// Joins the group (start() and drainAll() call this for you); takes over a stale duplicate session.
	public void register() {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("consumerId", consumerId);
		body.put("topics", topics);
		if (sessionTimeoutMs != null) body.put("sessionTimeoutMs", sessionTimeoutMs);
		try {
			logRegistration(http.post(groupPath() + "/consumers", body, RegistrationBody.class));
		} catch (StreamixApiException e) {
			if (!takeoverOnDuplicate || !e.is("DUPLICATE_CONSUMER")) throw e;
			log.warn("consumer id '{}' already live in group '{}'; taking over the session", consumerId, group);
			tryDeregister();
			logRegistration(http.post(groupPath() + "/consumers", body, RegistrationBody.class));
		}
	}

	private void registerUntilRunning() {
		while (running.get()) {
			try {
				register();
				return;
			} catch (StreamixException e) {
				log.warn("re-registration failed ({}); retrying", e.getMessage());
				sleepQuietly(retryBackoffMs);
			}
		}
	}

	public void deregister() {
		http.delete(consumerPath());
	}

	private void tryDeregister() {
		try {
			deregister();
		} catch (StreamixException ignored) {
			// already evicted / broker unreachable — nothing to clean up
		}
	}

	private void logRegistration(RegistrationBody r) {
		log.info("consumer '{}' joined group '{}' with {} assigned partition(s)",
				consumerId, group, r == null || r.assignedPartitions() == null ? 0 : r.assignedPartitions().size());
	}

	private String groupPath() { return "/api/v1/groups/" + HttpJson.seg(group); }

	private String consumerPath() { return groupPath() + "/consumers/" + HttpJson.seg(consumerId); }

	private static String defaultConsumerId() {
		String host;
		try {
			host = InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			host = "consumer";
		}
		return host + "-" + UUID.randomUUID().toString().substring(0, 8);
	}

	private static void sleepQuietly(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public record RegistrationBody(String group, String consumerId, long sessionTimeoutMs,
			List<AssignedPartition> assignedPartitions) {}

	public record AssignedPartition(String topic, int partition) {}

	public record PollBody(int count, List<ConsumedMessage> messages) {}

	public static final class Builder {

		private final String baseUrl;
		private String group;
		private String consumerId;
		private final Set<String> topics = new LinkedHashSet<>();
		private Long sessionTimeoutMs;
		private int pollMax = 100;
		private long waitMs = 10_000;
		private boolean autoCommit = true;
		private boolean takeoverOnDuplicate = true;
		private long retryBackoffMs = 1_000;
		private int maxRetries = 3;
		private long backoffMs = 200;
		private long requestTimeoutMs = 15_000;

		private Builder(String baseUrl) { this.baseUrl = baseUrl; }

		public Builder group(String v) { this.group = v; return this; }

		public Builder consumerId(String v) { this.consumerId = v; return this; }

		public Builder topics(String... v) { this.topics.addAll(List.of(v)); return this; }

		public Builder sessionTimeoutMs(long v) { this.sessionTimeoutMs = v; return this; }

		public Builder pollMax(int v) { this.pollMax = v; return this; }

		// 0 disables long polling (immediate empty responses).
		public Builder waitMs(long v) { this.waitMs = v; return this; }

		public Builder autoCommit(boolean v) { this.autoCommit = v; return this; }

		public Builder takeoverOnDuplicate(boolean v) { this.takeoverOnDuplicate = v; return this; }

		public Builder retryBackoffMs(long v) { this.retryBackoffMs = v; return this; }

		public StreamixConsumer build() {
			if (group == null || group.isBlank()) throw new IllegalArgumentException("group is required");
			if (topics.isEmpty()) throw new IllegalArgumentException("at least one topic is required");
			if (waitMs < 0 || pollMax < 1) throw new IllegalArgumentException("waitMs must be >= 0 and pollMax >= 1");
			return new StreamixConsumer(this);
		}
	}
}
