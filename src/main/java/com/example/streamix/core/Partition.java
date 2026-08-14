package com.example.streamix.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Append-only in-memory log; offset == list index while beginOffset is 0 (no trimming until retention, Phase 2).
public class Partition {

	private final ArrayList<Message> log = new ArrayList<>();
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private long nextOffset = 0;

	public Message append(String key, Object value, Map<String, String> headers, long timestamp) {
		lock.writeLock().lock();
		try {
			Message m = new Message(nextOffset++, key, value, headers, timestamp);
			log.add(m);
			return m;
		} finally {
			lock.writeLock().unlock();
		}
	}

	// Recovery path: re-insert a replayed message at its original offset.
	public void restore(Message m) {
		lock.writeLock().lock();
		try {
			if (m.offset() != nextOffset) {
				throw new IllegalStateException("non-contiguous offset " + m.offset() + ", expected " + nextOffset);
			}
			log.add(m);
			nextOffset = m.offset() + 1;
		} finally {
			lock.writeLock().unlock();
		}
	}

	public List<Message> read(long fromOffset, int max) {
		lock.readLock().lock();
		try {
			if (fromOffset >= nextOffset || max <= 0) return List.of();
			int from = (int) Math.max(fromOffset, 0);
			int to = (int) Math.min(nextOffset, fromOffset + max);
			return List.copyOf(log.subList(from, to));
		} finally {
			lock.readLock().unlock();
		}
	}

	public long endOffset() {
		lock.readLock().lock();
		try {
			return nextOffset;
		} finally {
			lock.readLock().unlock();
		}
	}

	public long beginOffset() { return 0; }
}
