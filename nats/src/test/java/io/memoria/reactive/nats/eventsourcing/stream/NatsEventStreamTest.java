package io.memoria.reactive.nats.eventsourcing.stream;

import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.testsuite.TestsuiteDefaults;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.stream.EventStreamScenario;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.NATS_CONFIG;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsEventStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsEventStreamTest.class.getName());
  private static final String topic = TestsuiteDefaults.topicName(NatsEventStreamTest.class);
  private static final int partition = 0;
  private static final EventStreamScenario scenario;

  static {
    try {
      var repo = new NatsEventStream<>(NATS_CONFIG,
                                       AccountEvent.class,
                                       TestsuiteDefaults.TRANSFORMER,
                                       TestsuiteDefaults.SCHEDULER);
      NatsUtils.createOrUpdateTopic(NATS_CONFIG, topic, 1).map(StreamInfo::toString).forEach(log::info);
      scenario = new EventStreamScenario(BankingData.ofUUID(), repo, TestsuiteDefaults.MSG_COUNT, topic, partition);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  @Order(0)
  void publish() {
    StepVerifier.create(scenario.publish()).expectNextCount(TestsuiteDefaults.MSG_COUNT).verifyComplete();
  }

  @Test
  @Order(1)
  void subscribe() {
    StepVerifier.create(scenario.subscribe())
                .expectNextCount(TestsuiteDefaults.MSG_COUNT)
                .expectTimeout(TestsuiteDefaults.TIMEOUT)
                .verify();
  }
}
