package com.example.streamix.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.example.streamix.config.BrokerProperties;
import com.example.streamix.group.GroupCoordinator;
import com.example.streamix.offset.NoopOffsetJournal;
import com.example.streamix.offset.OffsetStore;
import com.example.streamix.storage.InMemoryLogStorage;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class BrokerEngineTest {

	@Test
	void lagCountsOnlyRetainedMessages() {
		BrokerProperties props = new BrokerProperties();
		InMemoryLogStorage storage = new InMemoryLogStorage();
		TopicManager topicManager = new TopicManager(storage, props);
		OffsetStore offsetStore = new OffsetStore(new NoopOffsetJournal());
		GroupCoordinator coordinator = new GroupCoordinator(topicManager, offsetStore, storage, props, Clock.systemUTC());
		BrokerMetrics metrics = new BrokerMetrics(new SimpleMeterRegistry(), topicManager, coordinator, storage);
		BrokerEngine engine = new BrokerEngine(topicManager, new Partitioner(), storage, coordinator, offsetStore, props, metrics);

		engine.createTopic("t", 1, null, null);
		for (int i = 0; i < 4; i++) engine.publish("t", null, "v" + i, null);
		offsetStore.commit("g", "t", 0, 1); // consumer reached offset 1, then everything got trimmed
		storage.enforceRetention("t", 0, Long.MAX_VALUE, -1);

		List<GroupOffsetView> views = engine.groupOffsets("g", null);
		assertThat(views).hasSize(1);
		assertThat(views.get(0).committedOffset()).isEqualTo(1); // raw commit preserved
		assertThat(views.get(0).lag()).isZero(); // trimmed messages can never be consumed
	}
}
