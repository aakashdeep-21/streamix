package com.example.streamix.storage;

import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.streamix.config.BrokerProperties;
import com.example.streamix.core.BrokerMetrics;
import com.example.streamix.core.TopicManager;
import com.example.streamix.core.TopicMetadata;

// Retention is the only thing that deletes messages; consumption never does.
@Component
public class RetentionService {

	private static final Logger log = LoggerFactory.getLogger(RetentionService.class);

	private final TopicManager topicManager;
	private final LogStorage storage;
	private final BrokerProperties props;
	private final BrokerMetrics metrics;
	private final Clock clock;

	public RetentionService(TopicManager topicManager, LogStorage storage, BrokerProperties props,
			BrokerMetrics metrics, Clock clock) {
		this.topicManager = topicManager;
		this.storage = storage;
		this.props = props;
		this.metrics = metrics;
		this.clock = clock;
	}

	@Scheduled(fixedDelayString = "${streamix.retention-sweep-ms:60000}")
	public void sweep() {
		long start = System.currentTimeMillis();
		long totalRemoved = 0;
		int topicsChecked = 0;
		for (TopicMetadata meta : topicManager.list()) {
			long ageMs = meta.retentionMs() != null ? meta.retentionMs() : props.getRetentionMs();
			long maxBytes = meta.retentionBytes() != null ? meta.retentionBytes() : props.getRetentionBytes();
			if (ageMs <= 0 && maxBytes <= 0) continue;
			topicsChecked++;
			long minTimestamp = ageMs > 0 ? clock.millis() - ageMs : Long.MIN_VALUE;
			long removed = 0;
			for (int p = 0; p < meta.partitions(); p++) {
				try {
					removed += storage.enforceRetention(meta.name(), p, minTimestamp, maxBytes);
				} catch (IllegalStateException e) {
					break; // topic deleted mid-sweep
				}
			}
			if (removed > 0) {
				metrics.trimmed(removed);
				totalRemoved += removed;
				log.info("retention trimmed {} messages from topic '{}'", removed, meta.name());
			}
		}
		log.debug("retention sweep: {} topic(s) checked, {} message(s) trimmed in {}ms",
				topicsChecked, totalRemoved, System.currentTimeMillis() - start);
	}
}
