package com.example.streamix.api.dto;

import java.util.List;

import com.example.streamix.core.PartitionOffsets;

public record TopicDetailsResponse(String name, int partitions, long createdAt,
		Long retentionMs, Long retentionBytes, List<PartitionOffsets> offsets) {
}
