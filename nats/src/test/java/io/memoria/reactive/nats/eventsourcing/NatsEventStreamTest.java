package io.memoria.reactive.nats.eventsourcing;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.testsuite.TestsuiteUtils;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.natsConfig;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsEventStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsEventStreamTest.class.getName());
  private static final String topic = TestsuiteUtils.topicName(NatsEventStreamTest.class);
  private static final int partition = 0;
  private static final EventStream<AccountEvent> eventStream;
  private static final BankingData BANKING_DATA;

  static {
    try {
      eventStream = new NatsEventStream<>(natsConfig,
                                          AccountEvent.class,
                                          new SerializableTransformer(),
                                          Schedulers.boundedElastic());
      BANKING_DATA = BankingData.ofUUID();
      NatsUtils.createOrUpdateTopic(natsConfig, topic, 1).map(StreamInfo::toString).forEach(log::info);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publish() {
    // Given
    var ids = BANKING_DATA.createIds(0, TestsuiteUtils.MSG_COUNT);
    var events = ids.map(id -> BANKING_DATA.createAccountEvent(id, 500));
    // When
    var pub = events.flatMap(event -> eventStream.pub(topic, partition, event));
    // Then
    StepVerifier.create(pub).expectNextCount(TestsuiteUtils.MSG_COUNT).verifyComplete();
  }

  @Test
  @Order(1)
  void subscribe() {
    // When
    var sub = eventStream.sub(topic, partition);

    // Then
    StepVerifier.create(sub).expectNextCount(TestsuiteUtils.MSG_COUNT).expectTimeout(TestsuiteUtils.TIMEOUT).verify();
  }
}
