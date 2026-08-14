package com.example.streamix.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.example.streamix.config.BrokerProperties;
import com.example.streamix.core.Message;
import com.example.streamix.core.TopicMetadata;

import tools.jackson.databind.json.JsonMapper;

class FileLogStorageTest {

	private static final String SEG0 = "00000000000000000000.log";

	@TempDir
	Path dir;

	private FileLogStorage freshBroker(long segmentMaxBytes) {
		BrokerProperties props = new BrokerProperties();
		props.setDataDir(dir.toString());
		props.setSegmentMaxBytes(segmentMaxBytes);
		FileLogStorage storage = new FileLogStorage(props);
		storage.recover();
		return storage;
	}

	private FileLogStorage freshBroker() { return freshBroker(16_777_216); }

	@Test
	void messagesAndOffsetsSurviveRestart() {
		FileLogStorage s1 = freshBroker();
		s1.createLog(new TopicMetadata("orders", 2, 123L));
		s1.append("orders", 0, "k1", Map.of("amount", 42), Map.of("src", "test"));
		s1.append("orders", 0, null, "plain-string", null);
		s1.append("orders", 1, "k2", 7, null);
		s1.close();

		FileLogStorage s2 = freshBroker();
		assertThat(s2.recoveredTopics()).extracting(TopicMetadata::name).containsExactly("orders");
		assertThat(s2.endOffset("orders", 0)).isEqualTo(2);
		assertThat(s2.endOffset("orders", 1)).isEqualTo(1);

		List<Message> p0 = s2.read("orders", 0, 0, 10);
		assertThat(p0.get(0).key()).isEqualTo("k1");
		assertThat(p0.get(0).value()).isEqualTo(Map.of("amount", 42));
		assertThat(p0.get(0).headers()).isEqualTo(Map.of("src", "test"));
		assertThat(p0.get(1).value()).isEqualTo("plain-string");

		assertThat(s2.append("orders", 0, null, "after-restart", null).offset()).isEqualTo(2);
		s2.close();
	}

	@Test
	void rollsSegmentsAndReadsAcrossThem() throws Exception {
		FileLogStorage s = freshBroker(1); // one message per segment
		s.createLog(new TopicMetadata("t", 1, 1L));
		for (int i = 0; i < 10; i++) s.append("t", 0, null, "v" + i, null);

		try (var files = Files.list(dir.resolve("topics/t/0"))) {
			assertThat(files.count()).isEqualTo(10);
		}
		assertThat(s.read("t", 0, 0, 100)).extracting(Message::value)
				.containsExactly("v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9");
		assertThat(s.read("t", 0, 3, 4)).extracting(Message::offset).containsExactly(3L, 4L, 5L, 6L);
		s.close();
	}

	@Test
	void sparseIndexServesMidSegmentReads() {
		FileLogStorage s = freshBroker();
		s.createLog(new TopicMetadata("t", 1, 1L));
		for (int i = 0; i < 200; i++) s.append("t", 0, null, "v" + i, null);

		List<Message> slice = s.read("t", 0, 100, 5); // offset 100 = index slot 1 + 36 skipped lines
		assertThat(slice).extracting(Message::offset).containsExactly(100L, 101L, 102L, 103L, 104L);
		assertThat(slice).extracting(Message::value).containsExactly("v100", "v101", "v102", "v103", "v104");
		s.close();
	}

	@Test
	void trimByAgeKeepsOnlyActiveSegment() {
		FileLogStorage s = freshBroker(1);
		s.createLog(new TopicMetadata("t", 1, 1L));
		for (int i = 0; i < 5; i++) s.append("t", 0, null, "v" + i, null);

		long removed = s.enforceRetention("t", 0, Long.MAX_VALUE, -1);
		assertThat(removed).isEqualTo(4); // active segment is never trimmed
		assertThat(s.beginOffset("t", 0)).isEqualTo(4);
		assertThat(s.read("t", 0, 0, 10)).extracting(Message::value).containsExactly("v4"); // clamps to begin
		assertThat(s.totalSegments()).isEqualTo(1);
		s.close();
	}

	@Test
	void trimBySizeDropsOldestSealedSegments() {
		FileLogStorage s = freshBroker(1);
		s.createLog(new TopicMetadata("t", 1, 1L));
		for (int i = 0; i < 5; i++) s.append("t", 0, null, "v" + i, null);

		long removed = s.enforceRetention("t", 0, Long.MIN_VALUE, 1);
		assertThat(removed).isEqualTo(4);
		assertThat(s.beginOffset("t", 0)).isEqualTo(4);
		s.close();
	}

	@Test
	void recoveryAcrossSegmentsContinuesAppending() {
		FileLogStorage s1 = freshBroker(1);
		s1.createLog(new TopicMetadata("t", 1, 1L));
		for (int i = 0; i < 3; i++) s1.append("t", 0, null, "v" + i, null);
		s1.close();

		FileLogStorage s2 = freshBroker(1);
		assertThat(s2.endOffset("t", 0)).isEqualTo(3);
		assertThat(s2.read("t", 0, 0, 10)).hasSize(3);
		assertThat(s2.append("t", 0, null, "v3", null).offset()).isEqualTo(3);
		s2.close();
	}

	@Test
	void trimmedLogRecoversWithAdvancedBeginOffset() {
		FileLogStorage s1 = freshBroker(1);
		s1.createLog(new TopicMetadata("t", 1, 1L));
		for (int i = 0; i < 5; i++) s1.append("t", 0, null, "v" + i, null);
		s1.enforceRetention("t", 0, Long.MAX_VALUE, -1);
		s1.close();

		FileLogStorage s2 = freshBroker(1);
		assertThat(s2.beginOffset("t", 0)).isEqualTo(4);
		assertThat(s2.endOffset("t", 0)).isEqualTo(5);
		assertThat(s2.read("t", 0, 0, 10)).extracting(Message::value).containsExactly("v4");
		s2.close();
	}

	@Test
	void tornTailOnActiveSegmentIsTruncated() throws Exception {
		FileLogStorage s1 = freshBroker();
		s1.createLog(new TopicMetadata("t", 1, 1L));
		s1.append("t", 0, null, "a", null);
		s1.append("t", 0, null, "b", null);
		s1.close();

		Path segment = dir.resolve("topics/t/0/" + SEG0);
		Files.writeString(segment, "{\"offset\":2,\"key\":null,\"val", StandardOpenOption.APPEND);

		FileLogStorage s2 = freshBroker();
		assertThat(s2.endOffset("t", 0)).isEqualTo(2);
		assertThat(s2.append("t", 0, null, "c", null).offset()).isEqualTo(2);
		s2.close();

		assertThat(Files.readAllLines(segment, StandardCharsets.UTF_8)).hasSize(3);
	}

	@Test
	void corruptionInSealedSegmentFailsRecovery() throws Exception {
		FileLogStorage s1 = freshBroker(1);
		s1.createLog(new TopicMetadata("t", 1, 1L));
		s1.append("t", 0, null, "a", null);
		s1.append("t", 0, null, "b", null); // rolls; segment 0 is now sealed
		s1.close();

		Files.writeString(dir.resolve("topics/t/0/" + SEG0), "corrupted-not-json\n");
		assertThatThrownBy(() -> freshBroker(1)).isInstanceOf(IllegalStateException.class);
	}

	@Test
	void v1SingleFileLayoutIsMigrated() throws Exception {
		var mapper = JsonMapper.builder().build();
		Path topicDir = dir.resolve("topics/legacy");
		Files.createDirectories(topicDir);
		Files.writeString(topicDir.resolve("meta.json"),
				mapper.writeValueAsString(new TopicMetadata("legacy", 1, 1L)));
		String lines = mapper.writeValueAsString(new Message(0, "k", "old-1", null, 10L)) + "\n"
				+ mapper.writeValueAsString(new Message(1, null, "old-2", null, 11L)) + "\n";
		Files.writeString(topicDir.resolve("0.log"), lines);

		FileLogStorage s = freshBroker();
		assertThat(s.recoveredTopics()).extracting(TopicMetadata::name).containsExactly("legacy");
		assertThat(s.endOffset("legacy", 0)).isEqualTo(2);
		assertThat(s.read("legacy", 0, 0, 10)).extracting(Message::value).containsExactly("old-1", "old-2");
		assertThat(Files.exists(topicDir.resolve("0.log"))).isFalse();
		assertThat(Files.exists(topicDir.resolve("0/" + SEG0))).isTrue();
		assertThat(s.append("legacy", 0, null, "new", null).offset()).isEqualTo(2);
		s.close();
	}

	@Test
	void deleteLogRemovesTopicDirectory() {
		FileLogStorage s = freshBroker();
		s.createLog(new TopicMetadata("gone", 1, 1L));
		s.append("gone", 0, null, "x", null);
		assertThat(Files.exists(dir.resolve("topics/gone"))).isTrue();

		s.deleteLog("gone");
		assertThat(Files.exists(dir.resolve("topics/gone"))).isFalse();
		s.close();
	}
}
