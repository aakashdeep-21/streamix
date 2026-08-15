package com.example.streamix.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class PartitionTest {

	@Test
	void offsetsAreMonotonicFromZero() {
		Partition p = new Partition();
		assertThat(p.append("k", "v0", null, 1L).offset()).isEqualTo(0);
		assertThat(p.append("k", "v1", null, 2L).offset()).isEqualTo(1);
		assertThat(p.endOffset()).isEqualTo(2);
	}

	@Test
	void readReturnsWindowAndEmptyPastEnd() {
		Partition p = new Partition();
		for (int i = 0; i < 5; i++) p.append(null, "v" + i, null, i);
		assertThat(p.read(0, 2)).extracting(Message::offset).containsExactly(0L, 1L);
		assertThat(p.read(3, 100)).extracting(Message::offset).containsExactly(3L, 4L);
		assertThat(p.read(5, 10)).isEmpty();
		assertThat(p.read(99, 10)).isEmpty();
	}

	@Test
	void restoreRejectsNonContiguousOffsets() {
		Partition p = new Partition();
		p.restore(new Message(0, null, "a", null, 1L));
		org.assertj.core.api.Assertions.assertThatThrownBy(() -> p.restore(new Message(5, null, "b", null, 1L)))
				.isInstanceOf(IllegalStateException.class);
	}

	@Test
	void concurrentAppendsProduceDistinctSequentialOffsets() throws Exception {
		Partition p = new Partition();
		int threads = 8, perThread = 250;
		ExecutorService pool = Executors.newFixedThreadPool(threads);
		CountDownLatch start = new CountDownLatch(1);
		for (int t = 0; t < threads; t++) {
			pool.submit(() -> {
				start.await();
				for (int i = 0; i < perThread; i++) p.append(null, "v", null, 0L);
				return null;
			});
		}
		start.countDown();
		pool.shutdown();
		assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

		assertThat(p.endOffset()).isEqualTo((long) threads * perThread);
		List<Message> all = p.read(0, threads * perThread);
		assertThat(all).hasSize(threads * perThread);
		for (int i = 0; i < all.size(); i++) assertThat(all.get(i).offset()).isEqualTo(i);
	}
}
