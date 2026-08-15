package com.example.streamix.config;

import java.nio.file.Path;
import java.time.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.example.streamix.offset.FileOffsetJournal;
import com.example.streamix.offset.NoopOffsetJournal;
import com.example.streamix.offset.OffsetJournal;
import com.example.streamix.storage.FileLogStorage;
import com.example.streamix.storage.InMemoryLogStorage;
import com.example.streamix.storage.LogStorage;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;

// Storage mode wiring: file (durable, default) vs memory (tests/dev).
@Configuration
public class BrokerConfig {

	private static final Logger log = LoggerFactory.getLogger(BrokerConfig.class);

	@Bean
	Clock clock() { return Clock.systemUTC(); }

	@Bean
	OpenAPI streamixOpenApi() {
		return new OpenAPI().info(new Info()
				.title("Streamix Broker API")
				.description("Kafka-like REST message broker: topics, partitions, consumer groups, committed offsets. "
						+ "Delivery is at-least-once; ordering is per partition.")
				.version("v1"));
	}

	// Recovery runs here, before any dependent bean initializes.
	@Bean
	@ConditionalOnProperty(name = "streamix.storage", havingValue = "file", matchIfMissing = true)
	LogStorage fileLogStorage(BrokerProperties props) {
		log.info("storage: file-backed at '{}' (fsync={}, segmentMaxBytes={})",
				props.getDataDir(), props.getFsync(), props.getSegmentMaxBytes());
		FileLogStorage storage = new FileLogStorage(props);
		storage.recover();
		return storage;
	}

	@Bean
	@ConditionalOnProperty(name = "streamix.storage", havingValue = "memory")
	LogStorage inMemoryLogStorage() {
		log.warn("storage: in-memory — non-durable, all data is lost on restart (tests/dev only)");
		return new InMemoryLogStorage();
	}

	@Bean
	@ConditionalOnProperty(name = "streamix.storage", havingValue = "file", matchIfMissing = true)
	OffsetJournal fileOffsetJournal(BrokerProperties props) {
		return new FileOffsetJournal(Path.of(props.getDataDir()));
	}

	@Bean
	@ConditionalOnProperty(name = "streamix.storage", havingValue = "memory")
	OffsetJournal noopOffsetJournal() { return new NoopOffsetJournal(); }
}
