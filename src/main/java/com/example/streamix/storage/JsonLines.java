package com.example.streamix.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Helpers for append-only JSON-lines files (partition logs + offsets journal).
public final class JsonLines {

	private static final Logger log = LoggerFactory.getLogger(JsonLines.class);

	private JsonLines() {}

	// Feeds each line to the parser; a torn final line is truncated away, corruption elsewhere fails startup.
	public static void replay(Path file, Consumer<String> lineParser) throws IOException {
		if (!Files.exists(file)) return;
		List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
		long goodBytes = 0;
		for (int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			if (!line.isBlank()) {
				try {
					lineParser.accept(line);
				} catch (RuntimeException e) {
					if (i == lines.size() - 1) {
						log.warn("truncating torn tail of {} ({} -> {} bytes)", file, Files.size(file), goodBytes);
						try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
							ch.truncate(goodBytes);
						}
						return;
					}
					throw new IllegalStateException("corrupt file " + file + " at line " + (i + 1), e);
				}
			}
			goodBytes += line.getBytes(StandardCharsets.UTF_8).length + 1;
		}
	}

	// Append writer; repairs a missing trailing newline left by a crash mid-append.
	public static BufferedWriter openAppend(Path file) throws IOException {
		Files.createDirectories(file.getParent());
		if (Files.exists(file) && Files.size(file) > 0) {
			try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
				ByteBuffer last = ByteBuffer.allocate(1);
				ch.read(last, ch.size() - 1);
				if (last.get(0) != '\n') {
					Files.writeString(file, "\n", StandardOpenOption.APPEND);
				}
			}
		}
		return Files.newBufferedWriter(file, StandardCharsets.UTF_8,
				StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	}
}
