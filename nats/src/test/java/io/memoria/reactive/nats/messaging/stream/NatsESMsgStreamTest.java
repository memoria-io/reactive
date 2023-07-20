package io.memoria.reactive.nats.messaging.stream;

import io.memoria.reactive.core.messaging.stream.ESMsg;
import io.memoria.reactive.core.messaging.stream.ESMsgStream;
import io.memoria.reactive.nats.NatsConfig;
import io.vavr.collection.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.Random;

import static io.memoria.reactive.nats.TestUtils.NATS_URL;
import static io.memoria.reactive.nats.TestUtils.createConfig;

class NatsESMsgStreamTest {
  private static final int MSG_COUNT = 1000;
  private static final Random r = new Random();

  private final String topic;
  private final ESMsgStream repo;

  NatsESMsgStreamTest() throws IOException, InterruptedException {
    topic = "topic" + r.nextInt(1000);
    int totalPartitions = 1;
    repo = new NatsESMsgStream(new NatsConfig(NATS_URL, createConfig(topic, totalPartitions)));
  }

  @Test
  void publish() {
    // Given
    var partition = 0;
    var msgs = List.range(0, MSG_COUNT).map(i -> createEsMsg(topic, partition, String.valueOf(i)));
    // When
    var pub = Flux.fromIterable(msgs).concatMap(repo::pub);
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(repo.last(topic, partition)).expectNext(String.valueOf(MSG_COUNT - 1)).verifyComplete();
  }

  @Test
  void subscribe() {
    // Given
    var partition = 0;
    var msgs = List.range(0, MSG_COUNT).map(i -> createEsMsg(topic, partition, String.valueOf(i)));
    var pub = Flux.fromIterable(msgs).concatMap(repo::pub);

    // When
    var sub = repo.sub(topic, partition).take(MSG_COUNT);

    // Given
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextSequence(msgs).verifyComplete();
  }

  public static ESMsg createEsMsg(String topic, int partition, String key) {
    return new ESMsg(topic, partition, key, "hello_" + key);
  }
}
