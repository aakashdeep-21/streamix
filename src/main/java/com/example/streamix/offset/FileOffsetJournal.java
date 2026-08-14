package com.example.streamix.offset;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.function.BiConsumer;

import com.example.streamix.storage.JsonLines;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// Append-only JSON-lines journal of commits; replay is applied in order so last write wins.
public class FileOffsetJournal implements OffsetJournal {

	public record Entry(String group, String topic, int partition, long offset) {}

	private final ObjectMapper mapper = JsonMapper.builder().build();
	private final Path file;
	private BufferedWriter out; // guarded by this

	public FileOffsetJournal(Path dataDir) {
		this.file = dataDir.resolve("offsets.log");
	}

	@Override
	public synchronized void append(GroupTopicPartition key, long offset) {
		try {
			if (out == null) out = JsonLines.openAppend(file);
			out.write(mapper.writeValueAsString(new Entry(key.group(), key.topic(), key.partition(), offset)));
			out.write('\n');
			out.flush();
		} catch (IOException e) {
			throw new UncheckedIOException("offset journal append failed", e);
		}
	}

	@Override
	public void replay(BiConsumer<GroupTopicPartition, Long> sink) {
		try {
			JsonLines.replay(file, line -> {
				Entry e = mapper.readValue(line, Entry.class);
				sink.accept(new GroupTopicPartition(e.group(), e.topic(), e.partition()), e.offset());
			});
		} catch (IOException e) {
			throw new UncheckedIOException("offset journal replay failed", e);
		}
	}

	// Spring auto-invokes close() on context shutdown.
	public synchronized void close() {
		try {
			if (out != null) out.close();
		} catch (IOException ignored) {
			// best-effort close on shutdown
		}
	}
}
