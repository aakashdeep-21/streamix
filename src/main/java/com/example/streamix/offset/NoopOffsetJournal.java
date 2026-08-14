package com.example.streamix.offset;

import java.util.function.BiConsumer;

public class NoopOffsetJournal implements OffsetJournal {

	@Override
	public void append(GroupTopicPartition key, long offset) { }

	@Override
	public void replay(BiConsumer<GroupTopicPartition, Long> sink) { }
}
