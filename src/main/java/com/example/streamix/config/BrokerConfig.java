package com.example.streamix.config;

import java.nio.file.Path;
import java.time.Clock;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.streamix.offset.FileOffsetJournal;
import com.example.streamix.offset.NoopOffsetJournal;
import com.example.streamix.offset.OffsetJournal;
import com.example.streamix.storage.FileLogStorage;
import com.example.streamix.storage.InMemoryLogStorage;
import com.example.streamix.storage.LogStorage;

// Storage mode wiring: file (durable, default) vs memory (tests/dev).
@Configuration
public class BrokerConfig {

	@Bean
	Clock clock() { return Clock.systemUTC(); }

	// Recovery runs here, before any dependent bean initializes.
	@Bean
	@ConditionalOnProperty(name = "streamix.storage", havingValue = "file", matchIfMissing = true)
	LogStorage fileLogStorage(BrokerProperties props) {
		FileLogStorage storage = new FileLogStorage(Path.of(props.getDataDir()));
		storage.recover();
		return storage;
	}

	@Bean
	@ConditionalOnProperty(name = "streamix.storage", havingValue = "memory")
	LogStorage inMemoryLogStorage() { return new InMemoryLogStorage(); }

	@Bean
	@ConditionalOnProperty(name = "streamix.storage", havingValue = "file", matchIfMissing = true)
	OffsetJournal fileOffsetJournal(BrokerProperties props) {
		return new FileOffsetJournal(Path.of(props.getDataDir()));
	}

	@Bean
	@ConditionalOnProperty(name = "streamix.storage", havingValue = "memory")
	OffsetJournal noopOffsetJournal() { return new NoopOffsetJournal(); }
}
