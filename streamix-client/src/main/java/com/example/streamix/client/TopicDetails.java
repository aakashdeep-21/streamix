package com.example.streamix.client;

import java.util.List;

public record TopicDetails(String name, int partitions, long createdAt, Long retentionMs, Long retentionBytes,
		List<PartitionOffsets> offsets) {

	public record PartitionOffsets(int partition, long beginOffset, long endOffset) {}
}
