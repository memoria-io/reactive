package io.memoria.reactive.nats.eventsourcing;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.Data;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.nats.TestUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

class NatsEventStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsEventStreamTest.class.getName());

  private static final Duration timeout = Duration.ofMillis(1000);
  private static final int COUNT = 10000;
  private static final Random r = new Random();

  private final String topic = "topic" + r.nextInt(1000);
  private final int partition = 0;
  private final EventStream<AccountEvent> eventStream;
  private final Data data;

  NatsEventStreamTest() throws IOException, InterruptedException, JetStreamApiException {
    var natsConfig = TestUtils.natsConfig();
    this.eventStream = new NatsEventStream<>(natsConfig,
                                             AccountEvent.class,
                                             new SerializableTransformer(),
                                             Schedulers.boundedElastic());
    this.data = Data.ofUUID();
    NatsUtils.createOrUpdateTopic(natsConfig, topic, 1).map(StreamInfo::toString).forEach(log::info);
  }

  @Test
  void publish() {
    // Given
    var ids = data.createIds(0, COUNT);
    var events = ids.map(id -> data.createAccountEvent(id, 500));
    // When
    var pub = events.concatMap(event -> eventStream.pub(topic, partition, event));
    // Then
    StepVerifier.create(pub).expectNextCount(COUNT).verifyComplete();
  }

  @Test
  void subscribe() {
    // Given
    var ids = data.createIds(0, COUNT);
    var events = ids.map(id -> data.createAccountEvent(id, 500));
    // When
    var pub = events.concatMap(event -> eventStream.pub(topic, partition, event));
    var sub = eventStream.sub(topic, partition);

    // Then
    StepVerifier.create(pub).expectNextCount(COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(COUNT).expectTimeout(timeout).verify();
  }
}
