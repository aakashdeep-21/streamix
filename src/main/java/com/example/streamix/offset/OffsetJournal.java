package com.example.streamix.offset;

import java.util.Map;
import java.util.function.BiConsumer;

// Durability hook for commits; Noop for in-memory mode, file-backed in production.
public interface OffsetJournal {

	void append(GroupTopicPartition key, long offset);

	void replay(BiConsumer<GroupTopicPartition, Long> sink);

	// Compacts durable state into a snapshot so the journal can restart empty; no-op by default.
	default void snapshot(Map<GroupTopicPartition, Long> state) { }
}
