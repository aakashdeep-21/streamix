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
	private long beginOffset = 0;

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
			long from = Math.max(fromOffset, beginOffset);
			if (from >= nextOffset || max <= 0) return List.of();
			int fromIdx = (int) (from - beginOffset);
			int toIdx = (int) Math.min(log.size(), fromIdx + (long) max);
			return List.copyOf(log.subList(fromIdx, toIdx));
		} finally {
			lock.readLock().unlock();
		}
	}

	// Retention path: drop head messages older than the cutoff; size-based trim is file-storage-only.
	public long trimOlderThan(long minTimestampMs) {
		lock.writeLock().lock();
		try {
			int k = 0;
			while (k < log.size() && log.get(k).timestamp() < minTimestampMs) k++;
			if (k > 0) {
				log.subList(0, k).clear();
				beginOffset += k;
			}
			return k;
		} finally {
			lock.writeLock().unlock();
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

	public long beginOffset() {
		lock.readLock().lock();
		try {
			return beginOffset;
		} finally {
			lock.readLock().unlock();
		}
	}
}
