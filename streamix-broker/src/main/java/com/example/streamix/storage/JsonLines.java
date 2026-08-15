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

// Helpers for append-only JSON-lines files (segments + offsets journal/snapshot).
public final class JsonLines {

	private static final Logger log = LoggerFactory.getLogger(JsonLines.class);

	private JsonLines() {}

	public interface LineSink {
		void accept(String line, long startPos);
	}

	// Feeds each line + its byte offset to the sink; returns the clean byte length.
	// A torn final line is truncated when repairTornTail, otherwise any bad line fails.
	public static long replay(Path file, boolean repairTornTail, LineSink sink) throws IOException {
		if (!Files.exists(file)) return 0;
		List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
		long goodBytes = 0;
		for (int i = 0; i < lines.size(); i++) {
			String line = lines.get(i);
			if (!line.isBlank()) {
				try {
					sink.accept(line, goodBytes);
				} catch (RuntimeException e) {
					if (repairTornTail && i == lines.size() - 1) {
						log.warn("truncating torn tail of {} ({} -> {} bytes)", file, Files.size(file), goodBytes);
						try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
							ch.truncate(goodBytes);
						}
						return goodBytes;
					}
					throw new IllegalStateException("corrupt file " + file + " at line " + (i + 1), e);
				}
			}
			goodBytes += line.getBytes(StandardCharsets.UTF_8).length + 1;
		}
		return goodBytes;
	}

	public static void replay(Path file, Consumer<String> parser) throws IOException {
		replay(file, true, (line, pos) -> parser.accept(line));
	}

	// Heals a file whose final append crashed between the payload and its newline.
	public static void repairTrailingNewline(Path file) throws IOException {
		if (Files.exists(file) && Files.size(file) > 0) {
			try (FileChannel ch = FileChannel.open(file, StandardOpenOption.READ)) {
				ByteBuffer last = ByteBuffer.allocate(1);
				ch.read(last, ch.size() - 1);
				if (last.get(0) != '\n') {
					Files.writeString(file, "\n", StandardOpenOption.APPEND);
				}
			}
		}
	}

	public static BufferedWriter openAppend(Path file) throws IOException {
		Files.createDirectories(file.getParent());
		repairTrailingNewline(file);
		return Files.newBufferedWriter(file, StandardCharsets.UTF_8,
				StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	}
}
