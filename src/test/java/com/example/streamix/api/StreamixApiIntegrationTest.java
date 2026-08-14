package com.example.streamix.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.example.streamix.core.GroupOffsetView;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// Full lifecycle against the real HTTP layer (in-memory storage): ordered, shared broker state.
@SpringBootTest(properties = "streamix.storage=memory")
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StreamixApiIntegrationTest {

	private record PolledView(String topic, int partition, long offset) {}
	private record PollView(int count, List<PolledView> messages) {}

	private static final ObjectMapper json = JsonMapper.builder()
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.build();

	// carried across ordered tests
	private static Map<Integer, Long> nextOffsetByPartition = new HashMap<>();
	private static Set<String> firstUncommittedBatch;

	@Autowired
	private MockMvc mvc;

	@Test
	@Order(1)
	void createTopic() throws Exception {
		mvc.perform(post("/api/v1/topics").contentType(MediaType.APPLICATION_JSON)
						.content("{\"name\":\"orders\",\"partitions\":3}"))
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.name").value("orders"))
				.andExpect(jsonPath("$.partitions").value(3));
	}

	@Test
	@Order(2)
	void duplicateTopicRejected() throws Exception {
		mvc.perform(post("/api/v1/topics").contentType(MediaType.APPLICATION_JSON)
						.content("{\"name\":\"orders\",\"partitions\":3}"))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.error").value("DUPLICATE_TOPIC"));
	}

	@Test
	@Order(3)
	void invalidPartitionCountRejected() throws Exception {
		mvc.perform(post("/api/v1/topics").contentType(MediaType.APPLICATION_JSON)
						.content("{\"name\":\"bad\",\"partitions\":0}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.error").value("INVALID_ARGUMENT"));
	}

	@Test
	@Order(4)
	void publishToUnknownTopicRejected() throws Exception {
		mvc.perform(post("/api/v1/topics/ghost/messages").contentType(MediaType.APPLICATION_JSON)
						.content("{\"value\":{\"a\":1}}"))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.error").value("UNKNOWN_TOPIC"));
	}

	@Test
	@Order(5)
	void publishSingleReturnsPartitionAndOffset() throws Exception {
		mvc.perform(post("/api/v1/topics/orders/messages").contentType(MediaType.APPLICATION_JSON)
						.content("{\"key\":\"k1\",\"value\":{\"n\":1},\"headers\":{\"src\":\"it\"}}"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.topic").value("orders"))
				.andExpect(jsonPath("$.partition", allOf(greaterThanOrEqualTo(0), lessThan(3))))
				.andExpect(jsonPath("$.offset").value(0));
	}

	@Test
	@Order(6)
	void publishBatchAcksEachMessage() throws Exception {
		mvc.perform(post("/api/v1/topics/orders/messages/batch").contentType(MediaType.APPLICATION_JSON)
						.content("{\"messages\":[{\"value\":\"m1\"},{\"value\":\"m2\"},{\"value\":\"m3\"}]}"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.count").value(3))
				.andExpect(jsonPath("$.acks.length()").value(3));
	}

	@Test
	@Order(7)
	void publishWithoutValueRejected() throws Exception {
		mvc.perform(post("/api/v1/topics/orders/messages").contentType(MediaType.APPLICATION_JSON)
						.content("{\"key\":\"x\"}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.error").value("VALIDATION_FAILED"));
	}

	@Test
	@Order(8)
	void registerConsumerAssignsAllPartitions() throws Exception {
		mvc.perform(post("/api/v1/groups/g1/consumers").contentType(MediaType.APPLICATION_JSON)
						.content("{\"consumerId\":\"c1\",\"topics\":[\"orders\"]}"))
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.group").value("g1"))
				.andExpect(jsonPath("$.assignedPartitions.length()").value(3));
	}

	@Test
	@Order(9)
	void duplicateLiveConsumerRejected() throws Exception {
		mvc.perform(post("/api/v1/groups/g1/consumers").contentType(MediaType.APPLICATION_JSON)
						.content("{\"consumerId\":\"c1\",\"topics\":[\"orders\"]}"))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.error").value("DUPLICATE_CONSUMER"));
	}

	@Test
	@Order(10)
	void pollDeliversEverythingPublished() throws Exception {
		String body = mvc.perform(get("/api/v1/groups/g1/consumers/c1/messages").param("max", "100"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.count").value(4))
				.andReturn().getResponse().getContentAsString();
		PollView view = json.readValue(body, PollView.class);
		for (PolledView m : view.messages()) {
			nextOffsetByPartition.merge(m.partition(), m.offset() + 1, Math::max);
		}
	}

	@Test
	@Order(11)
	void secondPollIsEmptyWhileCaughtUp() throws Exception {
		mvc.perform(get("/api/v1/groups/g1/consumers/c1/messages"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.count").value(0));
	}

	@Test
	@Order(12)
	void commitOffsets() throws Exception {
		String entries = nextOffsetByPartition.entrySet().stream()
				.map(e -> "{\"topic\":\"orders\",\"partition\":" + e.getKey() + ",\"offset\":" + e.getValue() + "}")
				.collect(Collectors.joining(","));
		mvc.perform(post("/api/v1/groups/g1/consumers/c1/offsets").contentType(MediaType.APPLICATION_JSON)
						.content("{\"offsets\":[" + entries + "]}"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.committed").value(nextOffsetByPartition.size()));
	}

	@Test
	@Order(13)
	void groupOffsetsShowZeroLagAfterCommit() throws Exception {
		String body = mvc.perform(get("/api/v1/groups/g1/offsets"))
				.andExpect(status().isOk())
				.andReturn().getResponse().getContentAsString();
		GroupOffsetView[] views = json.readValue(body, GroupOffsetView[].class);
		assertThat(views).isNotEmpty();
		assertThat(views).allSatisfy(v -> assertThat(v.lag()).isZero());
	}

	@Test
	@Order(14)
	void uncommittedMessagesRedeliveredAfterReRegister() throws Exception {
		mvc.perform(post("/api/v1/topics/orders/messages/batch").contentType(MediaType.APPLICATION_JSON)
						.content("{\"messages\":[{\"value\":\"u1\"},{\"value\":\"u2\"}]}"))
				.andExpect(status().isOk());

		String first = mvc.perform(get("/api/v1/groups/g1/consumers/c1/messages").param("max", "100"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.count").value(2))
				.andReturn().getResponse().getContentAsString();

		mvc.perform(delete("/api/v1/groups/g1/consumers/c1")).andExpect(status().isNoContent());
		mvc.perform(post("/api/v1/groups/g1/consumers").contentType(MediaType.APPLICATION_JSON)
						.content("{\"consumerId\":\"c1\",\"topics\":[\"orders\"]}"))
				.andExpect(status().isCreated());

		String second = mvc.perform(get("/api/v1/groups/g1/consumers/c1/messages").param("max", "100"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.count").value(2))
				.andReturn().getResponse().getContentAsString();

		firstUncommittedBatch = toKeySet(first);
		assertThat(toKeySet(second)).isEqualTo(firstUncommittedBatch); // exact same messages again
	}

	@Test
	@Order(15)
	void commitOnUnassignedPartitionRejected() throws Exception {
		mvc.perform(post("/api/v1/groups/g1/consumers/c1/offsets").contentType(MediaType.APPLICATION_JSON)
						.content("{\"offsets\":[{\"topic\":\"orders\",\"partition\":99,\"offset\":0}]}"))
				.andExpect(status().isConflict())
				.andExpect(jsonPath("$.error").value("PARTITION_NOT_ASSIGNED"));
	}

	@Test
	@Order(16)
	void commitBeyondEndOffsetRejected() throws Exception {
		mvc.perform(post("/api/v1/groups/g1/consumers/c1/offsets").contentType(MediaType.APPLICATION_JSON)
						.content("{\"offsets\":[{\"topic\":\"orders\",\"partition\":0,\"offset\":9999}]}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.error").value("INVALID_OFFSET"));
	}

	@Test
	@Order(17)
	void pollAsUnknownConsumerRejected() throws Exception {
		mvc.perform(get("/api/v1/groups/g1/consumers/nobody/messages"))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.error").value("UNKNOWN_CONSUMER"));
		mvc.perform(get("/api/v1/groups/no-such-group/consumers/c/messages"))
				.andExpect(status().isNotFound());
	}

	@Test
	@Order(18)
	void invalidSessionTimeoutRejected() throws Exception {
		mvc.perform(post("/api/v1/groups/g1/consumers").contentType(MediaType.APPLICATION_JSON)
						.content("{\"consumerId\":\"c9\",\"topics\":[\"orders\"],\"sessionTimeoutMs\":5}"))
				.andExpect(status().isBadRequest())
				.andExpect(jsonPath("$.error").value("INVALID_ARGUMENT"));
	}

	@Test
	@Order(19)
	void secondConsumerTriggersRebalanceSplit() throws Exception {
		mvc.perform(post("/api/v1/groups/g1/consumers").contentType(MediaType.APPLICATION_JSON)
						.content("{\"consumerId\":\"c2\",\"topics\":[\"orders\"]}"))
				.andExpect(status().isCreated())
				.andExpect(jsonPath("$.assignedPartitions.length()", allOf(greaterThanOrEqualTo(1), lessThan(3))));
	}

	@Test
	@Order(20)
	void independentGroupReadsFromTheBeginning() throws Exception {
		mvc.perform(post("/api/v1/groups/analytics/consumers").contentType(MediaType.APPLICATION_JSON)
						.content("{\"consumerId\":\"a1\",\"topics\":[\"orders\"]}"))
				.andExpect(status().isCreated());
		mvc.perform(get("/api/v1/groups/analytics/consumers/a1/messages").param("max", "100"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.count").value(6)); // every message ever published
	}

	@Test
	@Order(21)
	void unknownRouteGetsUniformErrorBody() throws Exception {
		mvc.perform(get("/api/v1/nope"))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.status").value(404));
	}

	@Test
	@Order(22)
	void actuatorHealthIsUp() throws Exception {
		mvc.perform(get("/actuator/health"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.status").value("UP"));
	}

	@Test
	@Order(23)
	void actuatorExposesBrokerMetrics() throws Exception {
		mvc.perform(get("/actuator/metrics/streamix.messages.published"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.measurements[0].value", greaterThanOrEqualTo(6.0)));
	}

	@Test
	@Order(24)
	void deleteTopicCleansUpEverything() throws Exception {
		mvc.perform(delete("/api/v1/topics/orders")).andExpect(status().isNoContent());
		mvc.perform(get("/api/v1/topics/orders"))
				.andExpect(status().isNotFound())
				.andExpect(jsonPath("$.error").value("UNKNOWN_TOPIC"));

		// consumers stay registered but hold no assignments; offsets are purged
		mvc.perform(get("/api/v1/groups/analytics/consumers/a1/messages"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.count").value(0));
		mvc.perform(get("/api/v1/groups/g1/offsets"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.length()").value(0));
	}

	private static Set<String> toKeySet(String pollBody) {
		return json.readValue(pollBody, PollView.class).messages().stream()
				.map(m -> m.topic() + "-" + m.partition() + "@" + m.offset())
				.collect(Collectors.toSet());
	}
}
