package com.example.streamix.api;

import java.util.List;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.streamix.api.dto.BatchPublishRequest;
import com.example.streamix.api.dto.BatchPublishResponse;
import com.example.streamix.api.dto.PublishRequest;
import com.example.streamix.core.BrokerEngine;
import com.example.streamix.core.ProducerRecord;
import com.example.streamix.core.PublishResult;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/topics/{topic}/messages")
public class ProducerController {

	private final BrokerEngine engine;

	public ProducerController(BrokerEngine engine) {
		this.engine = engine;
	}

	@PostMapping
	public PublishResult publish(@PathVariable String topic, @Valid @RequestBody PublishRequest req) {
		return engine.publish(topic, req.key(), req.value(), req.headers());
	}

	@PostMapping("/batch")
	public BatchPublishResponse publishBatch(@PathVariable String topic, @Valid @RequestBody BatchPublishRequest req) {
		List<ProducerRecord> records = req.messages().stream()
				.map(m -> new ProducerRecord(m.key(), m.value(), m.headers()))
				.toList();
		List<PublishResult> acks = engine.publishBatch(topic, records);
		return new BatchPublishResponse(acks.size(), acks);
	}
}
