package com.example.streamix.api;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.example.streamix.api.dto.CommitRequest;
import com.example.streamix.api.dto.CommitResponse;
import com.example.streamix.api.dto.PollResponse;
import com.example.streamix.api.dto.PolledMessageDto;
import com.example.streamix.api.dto.RegisterConsumerRequest;
import com.example.streamix.api.dto.RegistrationResponse;
import com.example.streamix.core.BrokerEngine;
import com.example.streamix.core.GroupOffsetView;
import com.example.streamix.group.OffsetCommit;
import com.example.streamix.group.RegistrationResult;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/groups/{group}")
public class ConsumerController {

	private final BrokerEngine engine;

	public ConsumerController(BrokerEngine engine) {
		this.engine = engine;
	}

	@PostMapping("/consumers")
	@ResponseStatus(HttpStatus.CREATED)
	public RegistrationResponse register(@PathVariable String group, @Valid @RequestBody RegisterConsumerRequest req) {
		RegistrationResult result = engine.register(group, req.consumerId(), req.topics(), req.sessionTimeoutMs());
		return new RegistrationResponse(group, req.consumerId(), result.sessionTimeoutMs(), result.assignedPartitions());
	}

	@DeleteMapping("/consumers/{consumerId}")
	@ResponseStatus(HttpStatus.NO_CONTENT)
	public void deregister(@PathVariable String group, @PathVariable String consumerId) {
		engine.deregister(group, consumerId);
	}

	// Poll = heartbeat; returns an empty list when the consumer is fully caught up.
	@GetMapping("/consumers/{consumerId}/messages")
	public PollResponse poll(@PathVariable String group, @PathVariable String consumerId,
			@RequestParam(required = false) Integer max) {
		List<PolledMessageDto> messages = engine.poll(group, consumerId, max).stream()
				.map(PolledMessageDto::from)
				.toList();
		return new PollResponse(messages.size(), messages);
	}

	@PostMapping("/consumers/{consumerId}/offsets")
	public CommitResponse commit(@PathVariable String group, @PathVariable String consumerId,
			@Valid @RequestBody CommitRequest req) {
		List<OffsetCommit> entries = req.offsets().stream()
				.map(o -> new OffsetCommit(o.topic(), o.partition(), o.offset()))
				.toList();
		engine.commit(group, consumerId, entries);
		return new CommitResponse(entries.size());
	}

	@GetMapping("/offsets")
	public List<GroupOffsetView> offsets(@PathVariable String group, @RequestParam(required = false) String topic) {
		return engine.groupOffsets(group, topic);
	}
}
