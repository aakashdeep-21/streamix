package com.example.streamix.core;

import org.springframework.stereotype.Component;

import com.example.streamix.group.GroupCoordinator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

// Broker throughput counters + live gauges, exposed via /actuator/metrics.
@Component
public class BrokerMetrics {

	private final Counter published;
	private final Counter consumed;
	private final Counter committed;

	public BrokerMetrics(MeterRegistry registry, TopicManager topicManager, GroupCoordinator coordinator) {
		this.published = registry.counter("streamix.messages.published");
		this.consumed = registry.counter("streamix.messages.consumed");
		this.committed = registry.counter("streamix.offsets.committed");
		registry.gauge("streamix.topics", topicManager, TopicManager::count);
		registry.gauge("streamix.groups", coordinator, GroupCoordinator::groupCount);
		registry.gauge("streamix.consumers", coordinator, GroupCoordinator::consumerCount);
	}

	public void published(int n) { published.increment(n); }

	public void consumed(int n) { consumed.increment(n); }

	public void committed(int n) { committed.increment(n); }
}
