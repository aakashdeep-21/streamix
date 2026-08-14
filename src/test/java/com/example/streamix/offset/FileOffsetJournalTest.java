package com.example.streamix.offset;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileOffsetJournalTest {

	@TempDir
	Path dir;

	@Test
	void replayAppliesLastWins() {
		FileOffsetJournal j1 = new FileOffsetJournal(dir);
		j1.append(new GroupTopicPartition("g", "orders", 0), 5);
		j1.append(new GroupTopicPartition("g", "orders", 0), 9);
		j1.append(new GroupTopicPartition("g", "billing", 1), 3);
		j1.close();

		Map<GroupTopicPartition, Long> out = new HashMap<>();
		new FileOffsetJournal(dir).replay(out::put);
		assertThat(out).containsExactlyInAnyOrderEntriesOf(Map.of(
				new GroupTopicPartition("g", "orders", 0), 9L,
				new GroupTopicPartition("g", "billing", 1), 3L));
	}

	@Test
	void tornTailIsIgnored() throws Exception {
		FileOffsetJournal j1 = new FileOffsetJournal(dir);
		j1.append(new GroupTopicPartition("g", "orders", 0), 7);
		j1.close();
		Files.writeString(dir.resolve("offsets.log"), "{\"group\":\"g\",\"top", StandardOpenOption.APPEND);

		Map<GroupTopicPartition, Long> out = new HashMap<>();
		new FileOffsetJournal(dir).replay(out::put);
		assertThat(out).containsOnlyKeys(new GroupTopicPartition("g", "orders", 0));
	}

	@Test
	void emptyJournalReplaysNothing() {
		Map<GroupTopicPartition, Long> out = new HashMap<>();
		new FileOffsetJournal(dir).replay(out::put);
		assertThat(out).isEmpty();
	}

	@Test
	void snapshotCompactsJournalAndReplayMergesBoth() {
		FileOffsetJournal j = new FileOffsetJournal(dir);
		j.append(new GroupTopicPartition("g", "t", 0), 5);
		j.append(new GroupTopicPartition("g", "t", 0), 9);

		j.snapshot(Map.of(
				new GroupTopicPartition("g", "t", 0), 9L,
				new GroupTopicPartition("g", "t2", 1), 3L));
		assertThat(Files.exists(dir.resolve("offsets.snapshot"))).isTrue();
		assertThat(Files.exists(dir.resolve("offsets.log"))).isFalse(); // journal restarts empty

		j.append(new GroupTopicPartition("g", "t", 0), 12);
		j.close();

		Map<GroupTopicPartition, Long> out = new HashMap<>();
		new FileOffsetJournal(dir).replay(out::put);
		assertThat(out).containsExactlyInAnyOrderEntriesOf(Map.of(
				new GroupTopicPartition("g", "t", 0), 12L, // journal entry wins over snapshot
				new GroupTopicPartition("g", "t2", 1), 3L));
	}

	@Test
	void emptyStateSnapshotIsValid() {
		FileOffsetJournal j = new FileOffsetJournal(dir);
		j.snapshot(Map.of());
		Map<GroupTopicPartition, Long> out = new HashMap<>();
		new FileOffsetJournal(dir).replay(out::put);
		assertThat(out).isEmpty();
	}
}
