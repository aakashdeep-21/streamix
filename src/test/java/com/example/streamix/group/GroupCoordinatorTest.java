package com.example.streamix.group;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.streamix.config.BrokerProperties;
import com.example.streamix.core.BrokerException;
import com.example.streamix.core.ErrorCode;
import com.example.streamix.core.TopicManager;
import com.example.streamix.offset.NoopOffsetJournal;
import com.example.streamix.offset.OffsetStore;
import com.example.streamix.storage.InMemoryLogStorage;

class GroupCoordinatorTest {

	private MutableClock clock;
	private InMemoryLogStorage storage;
	private TopicManager topicManager;
	private OffsetStore offsetStore;
	private GroupCoordinator coordinator;

	@BeforeEach
	void setUp() {
		BrokerProperties props = new BrokerProperties();
		clock = new MutableClock();
		storage = new InMemoryLogStorage();
		topicManager = new TopicManager(storage, props);
		offsetStore = new OffsetStore(new NoopOffsetJournal());
		coordinator = new GroupCoordinator(topicManager, offsetStore, storage, props, clock);
		topicManager.create("orders", 4, null, null);
	}

	private void publish(int partition, String value) {
		storage.append("orders", partition, null, value, null);
	}

	private static void assertCode(ThrowingCallable call, ErrorCode code) {
		assertThatThrownBy(call).isInstanceOfSatisfying(BrokerException.class,
				e -> assertThat(e.code()).isEqualTo(code));
	}

	@Test
	void soloConsumerOwnsAllPartitions() {
		RegistrationResult r = coordinator.register("g", "c1", Set.of("orders"), null);
		assertThat(r.assignedPartitions()).hasSize(4);
	}

	@Test
	void twoConsumersSplitPartitionsDisjointly() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		coordinator.register("g", "c2", Set.of("orders"), null);
		for (int p = 0; p < 4; p++) publish(p, "m" + p);

		Set<Integer> seen1 = coordinator.poll("g", "c1", 100).stream()
				.map(PolledMessage::partition).collect(Collectors.toSet());
		Set<Integer> seen2 = coordinator.poll("g", "c2", 100).stream()
				.map(PolledMessage::partition).collect(Collectors.toSet());

		assertThat(seen1).hasSize(2);
		assertThat(seen2).hasSize(2);
		assertThat(seen1).doesNotContainAnyElementsOf(seen2);
	}

	@Test
	void duplicateLiveRegistrationRejected() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		assertCode(() -> coordinator.register("g", "c1", Set.of("orders"), null), ErrorCode.DUPLICATE_CONSUMER);
	}

	@Test
	void expiredConsumerCanReRegister() {
		coordinator.register("g", "c1", Set.of("orders"), 1000L);
		clock.advance(1001);
		RegistrationResult r = coordinator.register("g", "c1", Set.of("orders"), 1000L);
		assertThat(r.assignedPartitions()).hasSize(4);
	}

	@Test
	void sessionTimeoutEvictsAndReassignsToSurvivor() {
		coordinator.register("g", "c1", Set.of("orders"), 1000L);
		coordinator.register("g", "c2", Set.of("orders"), 100_000L);
		for (int p = 0; p < 4; p++) publish(p, "m" + p);

		clock.advance(5000);
		coordinator.expireDeadConsumers();

		assertCode(() -> coordinator.poll("g", "c1", 10), ErrorCode.UNKNOWN_CONSUMER);
		Set<Integer> seen2 = coordinator.poll("g", "c2", 100).stream()
				.map(PolledMessage::partition).collect(Collectors.toSet());
		assertThat(seen2).containsExactlyInAnyOrder(0, 1, 2, 3);
	}

	@Test
	void pollAdvancesPositionWithinSession() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		publish(0, "a");
		publish(0, "b");
		assertThat(coordinator.poll("g", "c1", 10)).hasSize(2);
		assertThat(coordinator.poll("g", "c1", 10)).isEmpty();
	}

	@Test
	void uncommittedMessagesRedeliveredAfterRebalance() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		publish(0, "a");
		publish(0, "b");
		assertThat(coordinator.poll("g", "c1", 10)).hasSize(2); // read but never committed

		coordinator.register("g", "c2", Set.of("orders"), null);
		coordinator.deregister("g", "c2"); // two rebalances; positions reset to committed

		assertThat(coordinator.poll("g", "c1", 10)).hasSize(2); // redelivered → at-least-once
	}

	@Test
	void committedOffsetsSurviveRebalance() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		publish(0, "a");
		publish(0, "b");
		assertThat(coordinator.poll("g", "c1", 10)).hasSize(2);
		coordinator.commit("g", "c1", List.of(new OffsetCommit("orders", 0, 2)));

		coordinator.register("g", "c2", Set.of("orders"), null);
		coordinator.deregister("g", "c2");

		assertThat(coordinator.poll("g", "c1", 10)).isEmpty(); // resumes from committed offset 2
	}

	@Test
	void commitUnassignedPartitionRejected() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		assertCode(() -> coordinator.commit("g", "c1", List.of(new OffsetCommit("orders", 99, 0))),
				ErrorCode.PARTITION_NOT_ASSIGNED);
	}

	@Test
	void commitBeyondEndOffsetRejected() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		publish(0, "a");
		assertCode(() -> coordinator.commit("g", "c1", List.of(new OffsetCommit("orders", 0, 5))),
				ErrorCode.INVALID_OFFSET);
	}

	@Test
	void unknownConsumerAndGroupRejected() {
		assertCode(() -> coordinator.poll("nope", "nobody", 10), ErrorCode.UNKNOWN_CONSUMER);
		coordinator.register("g", "c1", Set.of("orders"), null);
		assertCode(() -> coordinator.poll("g", "nobody", 10), ErrorCode.UNKNOWN_CONSUMER);
		assertCode(() -> coordinator.deregister("g", "nobody"), ErrorCode.UNKNOWN_CONSUMER);
	}

	@Test
	void independentGroupsEachSeeAllMessages() {
		publish(0, "a");
		publish(1, "b");
		publish(2, "c");

		coordinator.register("g1", "c1", Set.of("orders"), null);
		assertThat(coordinator.poll("g1", "c1", 10)).hasSize(3);
		coordinator.commit("g1", "c1", List.of(new OffsetCommit("orders", 0, 1),
				new OffsetCommit("orders", 1, 1), new OffsetCommit("orders", 2, 1)));

		coordinator.register("g2", "cx", Set.of("orders"), null);
		assertThat(coordinator.poll("g2", "cx", 10)).hasSize(3); // unaffected by g1's commits
	}

	@Test
	void deregisterReassignsPartitionsToSurvivor() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		coordinator.register("g", "c2", Set.of("orders"), null);
		for (int p = 0; p < 4; p++) publish(p, "m" + p);

		coordinator.deregister("g", "c2");
		Set<Integer> seen = coordinator.poll("g", "c1", 100).stream()
				.map(PolledMessage::partition).collect(Collectors.toSet());
		assertThat(seen).containsExactlyInAnyOrder(0, 1, 2, 3);
	}

	@Test
	void registrationValidatesTopicAndTimeout() {
		assertCode(() -> coordinator.register("g", "c1", Set.of("ghost"), null), ErrorCode.UNKNOWN_TOPIC);
		assertCode(() -> coordinator.register("g", "c1", Set.of("orders"), 10L), ErrorCode.INVALID_ARGUMENT);
	}

	@Test
	void trimmedMessagesAreSkippedAndOffsetsContinue() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		publish(0, "old1");
		publish(0, "old2");
		assertThat(storage.enforceRetention("orders", 0, Long.MAX_VALUE, -1)).isEqualTo(2);

		assertThat(coordinator.poll("g", "c1", 10)).isEmpty(); // trimmed data is gone, not an error
		publish(0, "fresh");
		List<PolledMessage> polled = coordinator.poll("g", "c1", 10);
		assertThat(polled).hasSize(1);
		assertThat(polled.get(0).message().offset()).isEqualTo(2); // offsets never restart
	}

	@Test
	void topicDeletionUnassignsConsumers() {
		coordinator.register("g", "c1", Set.of("orders"), null);
		publish(0, "a");
		topicManager.delete("orders");
		coordinator.onTopicDeleted("orders");

		assertThat(coordinator.poll("g", "c1", 10)).isEmpty();
		assertCode(() -> coordinator.commit("g", "c1", List.of(new OffsetCommit("orders", 0, 1))),
				ErrorCode.PARTITION_NOT_ASSIGNED);
	}

	// Deterministic clock so eviction tests never sleep.
	private static final class MutableClock extends Clock {

		private Instant now = Instant.parse("2026-01-01T00:00:00Z");

		void advance(long ms) { now = now.plusMillis(ms); }

		@Override
		public ZoneId getZone() { return ZoneOffset.UTC; }

		@Override
		public Clock withZone(ZoneId zone) { return this; }

		@Override
		public Instant instant() { return now; }
	}
}
