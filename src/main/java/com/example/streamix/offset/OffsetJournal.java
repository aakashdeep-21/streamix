package com.example.streamix.offset;

import java.util.function.BiConsumer;

// Durability hook for commits; Noop for in-memory mode, file-backed in production.
public interface OffsetJournal {

	void append(GroupTopicPartition key, long offset);

	void replay(BiConsumer<GroupTopicPartition, Long> sink);
}
