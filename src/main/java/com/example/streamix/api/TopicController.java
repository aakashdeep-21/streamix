package com.example.streamix.api;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.example.streamix.api.dto.CreateTopicRequest;
import com.example.streamix.api.dto.TopicDetailsResponse;
import com.example.streamix.core.BrokerEngine;
import com.example.streamix.core.TopicMetadata;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/topics")
public class TopicController {

	private final BrokerEngine engine;

	public TopicController(BrokerEngine engine) {
		this.engine = engine;
	}

	@PostMapping
	@ResponseStatus(HttpStatus.CREATED)
	public TopicMetadata create(@Valid @RequestBody CreateTopicRequest req) {
		return engine.createTopic(req.name(), req.partitions());
	}

	@GetMapping
	public List<TopicMetadata> list() {
		return engine.listTopics();
	}

	@GetMapping("/{topic}")
	public TopicDetailsResponse describe(@PathVariable String topic) {
		TopicMetadata meta = engine.topic(topic);
		return new TopicDetailsResponse(meta.name(), meta.partitions(), meta.createdAt(), engine.topicOffsets(topic));
	}

	@DeleteMapping("/{topic}")
	@ResponseStatus(HttpStatus.NO_CONTENT)
	public void delete(@PathVariable String topic) {
		engine.deleteTopic(topic);
	}
}
