package com.example.streamix.group;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.streamix.config.BrokerProperties;
import com.example.streamix.core.BrokerException;
import com.example.streamix.core.Message;
import com.example.streamix.core.TopicManager;
import com.example.streamix.core.TopicMetadata;
import com.example.streamix.core.TopicPartition;
import com.example.streamix.offset.OffsetStore;
import com.example.streamix.storage.LogStorage;

// Owns membership, broker-side assignment and fetch positions; poll doubles as heartbeat.
@Component
public class GroupCoordinator {

	private static final Logger log = LoggerFactory.getLogger(GroupCoordinator.class);

	private final ConcurrentHashMap<String, ConsumerGroup> groups = new ConcurrentHashMap<>();
	private final TopicManager topicManager;
	private final OffsetStore offsetStore;
	private final LogStorage storage;
	private final BrokerProperties props;
	private final Clock clock;

	public GroupCoordinator(TopicManager topicManager, OffsetStore offsetStore, LogStorage storage,
			BrokerProperties props, Clock clock) {
		this.topicManager = topicManager;
		this.offsetStore = offsetStore;
		this.storage = storage;
		this.props = props;
		this.clock = clock;
	}

	public RegistrationResult register(String groupId, String consumerId, Set<String> topics, Long requestedTimeoutMs) {
		topics.forEach(topicManager::get); // 404 on any unknown topic
		long timeout = resolveTimeout(requestedTimeoutMs);
		long now = clock.millis();
		while (true) {
			ConsumerGroup group = groups.computeIfAbsent(groupId, ConsumerGroup::new);
			synchronized (group) {
				if (groups.get(groupId) != group) continue; // emptied + removed concurrently; retry
				ConsumerSession existing = group.members.get(consumerId);
				if (existing != null) {
					if (now - existing.lastSeenMs <= existing.sessionTimeoutMs) {
						throw BrokerException.duplicateConsumer(groupId, consumerId);
					}
					group.members.remove(consumerId); // expired zombie; replace it
				}
				group.members.put(consumerId, new ConsumerSession(consumerId, topics, timeout, now));
				rebalance(group);
				log.info("consumer '{}' joined group '{}' subscribing {} (sessionTimeoutMs={})",
						consumerId, groupId, topics, timeout);
				return new RegistrationResult(timeout, assignedTo(group, consumerId));
			}
		}
	}

	public void deregister(String groupId, String consumerId) {
		ConsumerGroup group = requireGroup(groupId, consumerId);
		synchronized (group) {
			if (group.members.remove(consumerId) == null) throw BrokerException.unknownConsumer(groupId, consumerId);
			if (group.members.isEmpty()) groups.remove(groupId, group);
			else rebalance(group);
		}
		log.info("consumer '{}' left group '{}'", consumerId, groupId);
	}

	public List<PolledMessage> poll(String groupId, String consumerId, int max) {
		ConsumerGroup group = requireGroup(groupId, consumerId);
		long epoch;
		List<TopicPartition> mine;
		Map<TopicPartition, Long> from = new HashMap<>();
		synchronized (group) {
			ConsumerSession session = requireSession(group, groupId, consumerId);
			session.lastSeenMs = clock.millis();
			epoch = group.epoch;
			mine = new ArrayList<>(assignedTo(group, consumerId));
			if (mine.isEmpty()) return List.of();
			Collections.rotate(mine, (int) (session.pollRotation++ % mine.size())); // fairness across partitions
			for (TopicPartition tp : mine) {
				from.put(tp, session.positions.computeIfAbsent(tp, k -> seedPosition(groupId, k)));
			}
		}
		List<PolledMessage> out = new ArrayList<>();
		Map<TopicPartition, Long> advanced = new HashMap<>();
		int budget = max;
		for (TopicPartition tp : mine) {
			if (budget <= 0) break;
			List<Message> batch;
			try {
				batch = storage.read(tp.topic(), tp.partition(), from.get(tp), budget);
			} catch (IllegalStateException e) {
				continue; // topic deleted mid-poll; assignment catches up on the next rebalance
			}
			if (batch.isEmpty()) continue;
			batch.forEach(m -> out.add(new PolledMessage(tp.topic(), tp.partition(), m)));
			budget -= batch.size();
			advanced.put(tp, batch.get(batch.size() - 1).offset() + 1);
		}
		// Write positions back only if no rebalance intervened; otherwise next poll reseeds from committed.
		synchronized (group) {
			ConsumerSession session = group.members.get(consumerId);
			if (session != null && group.epoch == epoch) advanced.forEach(session.positions::put);
		}
		return out;
	}

	public void commit(String groupId, String consumerId, List<OffsetCommit> entries) {
		ConsumerGroup group = requireGroup(groupId, consumerId);
		synchronized (group) {
			ConsumerSession session = requireSession(group, groupId, consumerId);
			session.lastSeenMs = clock.millis();
			for (OffsetCommit c : entries) {
				TopicPartition tp = new TopicPartition(c.topic(), c.partition());
				if (!consumerId.equals(group.assignment.get(tp))) {
					throw BrokerException.partitionNotAssigned(consumerId, c.topic(), c.partition());
				}
				long end = storage.endOffset(c.topic(), c.partition());
				if (c.offset() < 0 || c.offset() > end) {
					throw BrokerException.invalidOffset(
							"offset " + c.offset() + " out of range [0," + end + "] for " + c.topic() + "-" + c.partition());
				}
			}
		}
		// Journal IO stays outside the group lock; a racing rebalance may only cause re-delivery.
		entries.forEach(c -> offsetStore.commit(groupId, c.topic(), c.partition(), c.offset()));
	}

	@Scheduled(fixedDelayString = "${streamix.session-sweep-ms:1000}")
	public void expireDeadConsumers() {
		long now = clock.millis();
		for (ConsumerGroup group : groups.values()) {
			synchronized (group) {
				List<String> dead = group.members.values().stream()
						.filter(s -> now - s.lastSeenMs > s.sessionTimeoutMs)
						.map(s -> s.consumerId)
						.toList();
				if (dead.isEmpty()) continue;
				dead.forEach(group.members::remove);
				log.info("evicted {} from group '{}' (session timeout)", dead, group.groupId);
				if (group.members.isEmpty()) groups.remove(group.groupId, group);
				else rebalance(group);
			}
		}
	}

	public void onTopicDeleted(String topic) {
		for (ConsumerGroup group : groups.values()) {
			synchronized (group) {
				boolean touched = false;
				for (ConsumerSession s : group.members.values()) touched |= s.topics.remove(topic);
				if (touched) rebalance(group);
			}
		}
	}

	public int groupCount() { return groups.size(); }

	public int consumerCount() {
		return groups.values().stream().mapToInt(g -> g.members.size()).sum();
	}

	// Recompute round-robin assignment; callers hold the group monitor.
	private void rebalance(ConsumerGroup group) {
		group.epoch++;
		group.assignment.clear();
		group.members.values().forEach(s -> s.positions.clear()); // reseed from committed → at-least-once
		SortedSet<String> topics = new TreeSet<>();
		group.members.values().forEach(s -> topics.addAll(s.topics));
		for (String topic : topics) {
			TopicMetadata meta = topicManager.find(topic);
			if (meta == null) continue; // deleted since subscription
			List<String> subscribers = group.members.values().stream()
					.filter(s -> s.topics.contains(topic))
					.map(s -> s.consumerId)
					.sorted()
					.toList();
			for (int p = 0; p < meta.partitions(); p++) {
				group.assignment.put(new TopicPartition(topic, p), subscribers.get(p % subscribers.size()));
			}
		}
		if (log.isInfoEnabled()) {
			Map<String, List<String>> byConsumer = new TreeMap<>();
			group.assignment.forEach((tp, c) ->
					byConsumer.computeIfAbsent(c, k -> new ArrayList<>()).add(tp.topic() + "-" + tp.partition()));
			byConsumer.values().forEach(Collections::sort);
			log.info("group '{}' rebalanced (epoch {}, {} member(s)): {}",
					group.groupId, group.epoch, group.members.size(), byConsumer);
		}
	}

	private long seedPosition(String groupId, TopicPartition tp) {
		return offsetStore.fetch(groupId, tp.topic(), tp.partition())
				.orElseGet(() -> storage.beginOffset(tp.topic(), tp.partition()));
	}

	private List<TopicPartition> assignedTo(ConsumerGroup group, String consumerId) {
		return group.assignment.entrySet().stream()
				.filter(e -> e.getValue().equals(consumerId))
				.map(Map.Entry::getKey)
				.sorted()
				.toList();
	}

	private ConsumerGroup requireGroup(String groupId, String consumerId) {
		ConsumerGroup group = groups.get(groupId);
		if (group == null) throw BrokerException.unknownConsumer(groupId, consumerId);
		return group;
	}

	private ConsumerSession requireSession(ConsumerGroup group, String groupId, String consumerId) {
		ConsumerSession session = group.members.get(consumerId);
		if (session == null) throw BrokerException.unknownConsumer(groupId, consumerId);
		return session;
	}

	private long resolveTimeout(Long requested) {
		if (requested == null) return props.getDefaultSessionTimeoutMs();
		if (requested < props.getMinSessionTimeoutMs() || requested > props.getMaxSessionTimeoutMs()) {
			throw BrokerException.invalidArgument("sessionTimeoutMs must be between "
					+ props.getMinSessionTimeoutMs() + " and " + props.getMaxSessionTimeoutMs());
		}
		return requested;
	}
}
