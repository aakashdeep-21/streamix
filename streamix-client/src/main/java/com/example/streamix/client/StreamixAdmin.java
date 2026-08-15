package com.example.streamix.client;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Topic administration; ensureTopic is the idempotent service-startup helper.
public final class StreamixAdmin {

	private static final Logger log = LoggerFactory.getLogger(StreamixAdmin.class);

	private final HttpJson http;

	private StreamixAdmin(HttpJson http) {
		this.http = http;
	}

	public static StreamixAdmin create(String baseUrl) {
		return new StreamixAdmin(new HttpJson(baseUrl, 3, 200, Duration.ofSeconds(15)));
	}

	public TopicInfo createTopic(String name, int partitions) {
		return createTopic(name, partitions, null, null);
	}

	public TopicInfo createTopic(String name, int partitions, Long retentionMs, Long retentionBytes) {
		Map<String, Object> body = new LinkedHashMap<>();
		body.put("name", name);
		body.put("partitions", partitions);
		if (retentionMs != null) body.put("retentionMs", retentionMs);
		if (retentionBytes != null) body.put("retentionBytes", retentionBytes);
		TopicInfo info = http.post("/api/v1/topics", body, TopicInfo.class);
		log.info("created topic '{}' ({} partitions)", info.name(), info.partitions());
		return info;
	}

	// Creates the topic if missing; existing topics are left untouched (partition count is not reconciled).
	public TopicDetails ensureTopic(String name, int partitions) {
		try {
			createTopic(name, partitions);
		} catch (StreamixApiException e) {
			if (!e.is("DUPLICATE_TOPIC")) throw e;
			log.debug("topic '{}' already exists", name);
		}
		return describe(name);
	}

	public List<TopicInfo> listTopics() {
		return List.of(http.get("/api/v1/topics", TopicInfo[].class));
	}

	public TopicDetails describe(String name) {
		return http.get("/api/v1/topics/" + HttpJson.seg(name), TopicDetails.class);
	}

	public void deleteTopic(String name) {
		http.delete("/api/v1/topics/" + HttpJson.seg(name));
		log.info("deleted topic '{}'", name);
	}

	public List<GroupOffset> groupOffsets(String group) {
		return List.of(http.get("/api/v1/groups/" + HttpJson.seg(group) + "/offsets", GroupOffset[].class));
	}
}
