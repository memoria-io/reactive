package io.memoria.reactive.nats.messaging.stream;

import io.memoria.reactive.core.messaging.stream.ESMsg;
import io.memoria.reactive.core.messaging.stream.ESMsgStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.vavr.collection.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import static io.memoria.reactive.nats.Utils.NATS_URL;
import static io.memoria.reactive.nats.Utils.createConfig;

class NatsESMsgStreamTest {
  private static final Duration timeout = Duration.ofMillis(500);
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
    var sub = repo.sub(topic, partition);
    var subAgain = repo.sub(topic, partition);

    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(MSG_COUNT).expectTimeout(timeout).verify();
//    StepVerifier.create(subAgain.take(msgs.size())).expectNextSequence(msgs).verifyComplete();
//    StepVerifier.create(subAgain).expectNextCount(MSG_COUNT).expectTimeout(timeout).verify();
    StepVerifier.create(subAgain).expectNextSequence(msgs).expectTimeout(Duration.ofMillis(1000)).verify();
  }

  @Test
  void toMessage() {
    var message = NatsESMsgStream.toMessage(new ESMsg("topic", 0, 1000 + "", "hello world"));
    Assertions.assertEquals("1000", message.getHeaders().getFirst(NatsUtils.ID_HEADER));
    Assertions.assertEquals("topic_0.subject", message.getSubject());
  }

  @Test
  void toMsg() {
    var h = new Headers();
    h.add(NatsUtils.ID_HEADER, "1000");
    var message = NatsMessage.builder().data("hello world").subject("topic_0.subject").headers(h).build();
    var msg = NatsESMsgStream.toESMsg(message);
    Assertions.assertEquals("topic", msg.topic());
    Assertions.assertEquals(0, msg.partition());
  }

  public static ESMsg createEsMsg(String topic, int partition, String key) {
    return new ESMsg(topic, partition, key, "hello_" + key);
  }
}
