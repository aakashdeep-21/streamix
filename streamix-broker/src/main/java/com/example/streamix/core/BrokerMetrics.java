package com.example.streamix.core;

import org.springframework.stereotype.Component;

import com.example.streamix.group.GroupCoordinator;
import com.example.streamix.storage.LogStorage;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

// Broker throughput counters + live gauges, exposed via /actuator/metrics.
@Component
public class BrokerMetrics {

	private final Counter published;
	private final Counter consumed;
	private final Counter committed;
	private final Counter trimmed;

	public BrokerMetrics(MeterRegistry registry, TopicManager topicManager, GroupCoordinator coordinator,
			LogStorage storage) {
		this.published = registry.counter("streamix.messages.published");
		this.consumed = registry.counter("streamix.messages.consumed");
		this.committed = registry.counter("streamix.offsets.committed");
		this.trimmed = registry.counter("streamix.messages.trimmed");
		registry.gauge("streamix.topics", topicManager, TopicManager::count);
		registry.gauge("streamix.groups", coordinator, GroupCoordinator::groupCount);
		registry.gauge("streamix.consumers", coordinator, GroupCoordinator::consumerCount);
		registry.gauge("streamix.storage.retained.bytes", storage, LogStorage::totalRetainedBytes);
		registry.gauge("streamix.storage.segments", storage, LogStorage::totalSegments);
	}

	public void published(int n) { published.increment(n); }

	public void consumed(int n) { consumed.increment(n); }

	public void committed(int n) { committed.increment(n); }

	public void trimmed(long n) { trimmed.increment(n); }
}
