package com.example.streamix.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

// Key → stable hash partition (per-key ordering); no key → per-topic round-robin.
@Component
public class Partitioner {

	private final Map<String, AtomicInteger> roundRobin = new ConcurrentHashMap<>();

	public int partition(String topic, String key, int partitionCount) {
		if (key != null && !key.isEmpty()) {
			return Math.floorMod(key.hashCode(), partitionCount);
		}
		AtomicInteger counter = roundRobin.computeIfAbsent(topic, t -> new AtomicInteger());
		return Math.floorMod(counter.getAndIncrement(), partitionCount);
	}

	public void forget(String topic) { roundRobin.remove(topic); }
}
