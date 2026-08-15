package com.example.streamix.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.example.streamix.StreamixApplication;

// Boots the real broker (in-memory storage) and drives it exclusively through the client.
class StreamixClientE2ETest {

	private static ConfigurableApplicationContext broker;
	private static String baseUrl;
	private static StreamixAdmin admin;
	private static StreamixProducer producer;

	public record OrderEvent(int orderId, String status) {}

	@BeforeAll
	static void startBroker() throws IOException {
		int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			port = socket.getLocalPort();
		}
		broker = SpringApplication.run(StreamixApplication.class,
				"--server.port=" + port,
				"--streamix.storage=memory",
				"--streamix.session-sweep-ms=100",
				"--streamix.min-session-timeout-ms=500");
		baseUrl = "http://localhost:" + port;
		admin = StreamixAdmin.create(baseUrl);
		producer = StreamixProducer.create(baseUrl);
	}

	@AfterAll
	static void stopBroker() {
		if (broker != null) broker.close();
	}

	@Test
	void adminManagesTopics() {
		TopicDetails created = admin.ensureTopic("e2e-admin", 2);
		assertThat(created.partitions()).isEqualTo(2);
		assertThat(admin.ensureTopic("e2e-admin", 2).name()).isEqualTo("e2e-admin"); // idempotent
		assertThat(admin.listTopics()).extracting(TopicInfo::name).contains("e2e-admin");

		admin.deleteTopic("e2e-admin");
		assertThatThrownBy(() -> admin.describe("e2e-admin"))
				.isInstanceOfSatisfying(StreamixApiException.class, e -> assertThat(e.is("UNKNOWN_TOPIC")).isTrue());
	}

	@Test
	void produceDrainCommitResumeRoundTrip() throws Exception {
		admin.ensureTopic("e2e-rt", 2);
		PublishAck first = producer.send("e2e-rt", "user-1", Map.of("n", 1));
		PublishAck second = producer.send("e2e-rt", "user-1", Map.of("n", 2));
		assertThat(second.partition()).isEqualTo(first.partition()); // per-key ordering
		producer.sendBatch("e2e-rt", List.of(ProducerMessage.of("a"), ProducerMessage.of("b")));

		StreamixConsumer consumer = StreamixConsumer.builder(baseUrl)
				.group("g-rt").consumerId("rt-1").topics("e2e-rt").build();
		ConcurrentLinkedQueue<ConsumedMessage> seen = new ConcurrentLinkedQueue<>();
		assertThat(consumer.drainAll(seen::addAll)).isEqualTo(4);
		assertThat(consumer.drainAll(seen::addAll)).isZero(); // committed → nothing left

		producer.send("e2e-rt", Map.of("n", 3));
		assertThat(consumer.drainAll(seen::addAll)).isEqualTo(1); // resumes from committed offset
		assertThat(seen).hasSize(5);
	}

	@Test
	void typedValueMapping() {
		admin.ensureTopic("e2e-typed", 1);
		producer.send("e2e-typed", new OrderEvent(101, "PAID"));

		StreamixConsumer consumer = StreamixConsumer.builder(baseUrl)
				.group("g-typed").consumerId("t-1").topics("e2e-typed").build();
		ConcurrentLinkedQueue<ConsumedMessage> seen = new ConcurrentLinkedQueue<>();
		consumer.drainAll(seen::addAll);
		assertThat(seen.peek().valueAs(OrderEvent.class)).isEqualTo(new OrderEvent(101, "PAID"));
	}

	@Test
	void continuousConsumerReceivesViaLongPollAndCommits() throws Exception {
		admin.ensureTopic("e2e-live", 1);
		CountDownLatch received = new CountDownLatch(1);
		ConcurrentLinkedQueue<ConsumedMessage> seen = new ConcurrentLinkedQueue<>();

		try (StreamixConsumer consumer = StreamixConsumer.builder(baseUrl)
				.group("g-live").consumerId("live-1").topics("e2e-live").waitMs(3000).build()) {
			consumer.start(batch -> {
				seen.addAll(batch);
				received.countDown();
			});
			producer.send("e2e-live", "hello-live");
			assertThat(received.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(seen.peek().value()).isEqualTo("hello-live");

			// auto-commit lands shortly after the handler returns
			long deadline = System.currentTimeMillis() + 5000;
			while (System.currentTimeMillis() < deadline) {
				List<GroupOffset> offsets = admin.groupOffsets("g-live");
				if (!offsets.isEmpty() && offsets.get(0).lag() == 0) return;
				Thread.sleep(100);
			}
			throw new AssertionError("commit never observed for g-live");
		}
	}

	@Test
	void failedHandlerGetsSameBatchRedelivered() throws Exception {
		admin.ensureTopic("e2e-retry", 1);
		producer.send("e2e-retry", "poison-then-fine");
		AtomicInteger attempts = new AtomicInteger();
		CountDownLatch succeeded = new CountDownLatch(1);

		try (StreamixConsumer consumer = StreamixConsumer.builder(baseUrl)
				.group("g-retry").consumerId("retry-1").topics("e2e-retry")
				.waitMs(1000).retryBackoffMs(200).build()) {
			consumer.start(batch -> {
				if (attempts.incrementAndGet() == 1) throw new RuntimeException("simulated failure");
				succeeded.countDown();
			});
			assertThat(succeeded.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(attempts.get()).isGreaterThanOrEqualTo(2); // same message came back
		}
	}

	@Test
	void slowHandlerEvictionIsAutoRecovered() throws Exception {
		admin.ensureTopic("e2e-evict", 1);
		producer.send("e2e-evict", "slow-one");
		AtomicInteger deliveries = new AtomicInteger();
		CountDownLatch recovered = new CountDownLatch(1);

		try (StreamixConsumer consumer = StreamixConsumer.builder(baseUrl)
				.group("g-evict").consumerId("evict-1").topics("e2e-evict")
				.sessionTimeoutMs(500).waitMs(200).retryBackoffMs(100).build()) {
			consumer.start(batch -> {
				if (deliveries.incrementAndGet() == 1) {
					Thread.sleep(1200); // outlive the session; the sweeper evicts us mid-handling
				} else {
					recovered.countDown();
				}
			});
			assertThat(recovered.await(20, TimeUnit.SECONDS)).isTrue(); // re-registered + redelivered
		}
	}

	@Test
	void duplicateConsumerIdTakeoverAndStrictMode() {
		admin.ensureTopic("e2e-dup", 1);
		StreamixConsumer first = StreamixConsumer.builder(baseUrl)
				.group("g-dup").consumerId("dup-1").topics("e2e-dup").takeoverOnDuplicate(false).build();
		first.register();

		StreamixConsumer strict = StreamixConsumer.builder(baseUrl)
				.group("g-dup").consumerId("dup-1").topics("e2e-dup").takeoverOnDuplicate(false).build();
		assertThatThrownBy(strict::register)
				.isInstanceOfSatisfying(StreamixApiException.class, e -> assertThat(e.is("DUPLICATE_CONSUMER")).isTrue());

		StreamixConsumer takeover = StreamixConsumer.builder(baseUrl)
				.group("g-dup").consumerId("dup-1").topics("e2e-dup").build();
		takeover.register(); // default policy evicts the stale session and joins

		// exactly one live session remains (the broker cannot fence same-id callers apart)
		assertThatThrownBy(strict::register)
				.isInstanceOfSatisfying(StreamixApiException.class, e -> assertThat(e.is("DUPLICATE_CONSUMER")).isTrue());
		takeover.deregister();
	}
}
