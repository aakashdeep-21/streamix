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

import com.example.streamix.core.Message;
import com.example.streamix.core.TopicMetadata;

class FileLogStorageTest {

	@TempDir
	Path dir;

	private FileLogStorage freshBroker() {
		FileLogStorage s = new FileLogStorage(dir);
		s.recover();
		return s;
	}

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

		// appends continue at the recovered offset
		assertThat(s2.append("orders", 0, null, "after-restart", null).offset()).isEqualTo(2);
		s2.close();
	}

	@Test
	void tornTailIsTruncatedOnRecovery() throws Exception {
		FileLogStorage s1 = freshBroker();
		s1.createLog(new TopicMetadata("t", 1, 1L));
		s1.append("t", 0, null, "a", null);
		s1.append("t", 0, null, "b", null);
		s1.close();

		Path logFile = dir.resolve("topics/t/0.log");
		Files.writeString(logFile, "{\"offset\":2,\"key\":null,\"val", StandardOpenOption.APPEND);

		FileLogStorage s2 = freshBroker();
		assertThat(s2.endOffset("t", 0)).isEqualTo(2);
		assertThat(s2.append("t", 0, null, "c", null).offset()).isEqualTo(2);
		s2.close();

		List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
		assertThat(lines).hasSize(3); // a, b, c — the torn record is gone
	}

	@Test
	void corruptionInTheMiddleFailsRecovery() throws Exception {
		FileLogStorage s1 = freshBroker();
		s1.createLog(new TopicMetadata("t", 1, 1L));
		s1.append("t", 0, null, "a", null);
		s1.append("t", 0, null, "b", null);
		s1.close();

		Path logFile = dir.resolve("topics/t/0.log");
		List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
		Files.write(logFile, List.of(lines.get(0), "not-json-at-all", lines.get(1)));

		assertThatThrownBy(() -> freshBroker()).isInstanceOf(IllegalStateException.class);
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
