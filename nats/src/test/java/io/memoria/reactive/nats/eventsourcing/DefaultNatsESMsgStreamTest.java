package io.memoria.reactive.nats.eventsourcing;

import io.memoria.reactive.core.stream.ESMsgStream;
import io.nats.client.Nats;
import io.vavr.collection.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.Random;

class DefaultNatsESMsgStreamTest {
  private static final String natsUrl = "nats://localhost:4222";
  private static final int MSG_COUNT = 1000;
  private static final Random r = new Random();

  private final String topic = "topic" + r.nextInt(1000);
  private final int topicTotalPartitions = 1;
  private final ESMsgStream repo = createRepo(topic, topicTotalPartitions);

  @Test
  void publish() {
    // Given
    var partition = 0;
    var msgs = List.range(0, MSG_COUNT).map(i -> TestUtils.createEsMsg(topic, partition, String.valueOf(i)));
    // When
    var pub = Flux.fromIterable(msgs).concatMap(repo::pub);
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(repo.lastKey(topic, partition)).expectNext(String.valueOf(MSG_COUNT - 1)).verifyComplete();
  }

  @Test
  void subscribe() {
    // Given
    var partition = 0;
    var msgs = List.range(0, MSG_COUNT).map(i -> TestUtils.createEsMsg(topic, partition, String.valueOf(i)));
    var pub = Flux.fromIterable(msgs).concatMap(repo::pub);

    // When
    var sub = repo.sub(topic, partition).take(MSG_COUNT);

    // Given
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextSequence(msgs).verifyComplete();
  }

  private ESMsgStream createRepo(String topic, int nTotalPartitions) {
    var natsConfig = new NatsConfig(natsUrl, TestUtils.createConfigs(topic, nTotalPartitions));
    try {
      var nc = Nats.connect(NatsUtils.toOptions(natsConfig));
      return NatsESMsgStream.create(nc, natsConfig);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
