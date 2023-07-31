package io.memoria.reactive.nats.eventsourcing.stream;

import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.testsuite.TestsuiteUtils;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.stream.CommandStreamScenario;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.NATS_CONFIG;
import static io.memoria.reactive.testsuite.TestsuiteUtils.MSG_COUNT;
import static io.memoria.reactive.testsuite.TestsuiteUtils.SCHEDULER;
import static io.memoria.reactive.testsuite.TestsuiteUtils.TRANSFORMER;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsCommandStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandStreamTest.class.getName());
  private static final String topic = TestsuiteUtils.topicName(NatsCommandStreamTest.class);
  private static final int partition = 0;
  private static final CommandStreamScenario scenario;

  static {
    try {
      var repo = new NatsCommandStream<>(NATS_CONFIG, AccountCommand.class, TRANSFORMER, SCHEDULER);
      NatsUtils.createOrUpdateTopic(NATS_CONFIG, topic, 1).map(StreamInfo::toString).forEach(log::info);
      scenario = new CommandStreamScenario(BankingData.ofUUID(), repo, MSG_COUNT, topic, partition);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void publish() {
    StepVerifier.create(scenario.publish()).expectNextCount(MSG_COUNT).verifyComplete();
  }

  @Test
  void subscribe() {
    StepVerifier.create(scenario.subscribe()).expectNextCount(MSG_COUNT).expectTimeout(TestsuiteUtils.TIMEOUT).verify();
  }
}
