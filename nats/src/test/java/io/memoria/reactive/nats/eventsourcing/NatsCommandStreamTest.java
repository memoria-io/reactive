package io.memoria.reactive.nats.eventsourcing;

import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.Data;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.nats.TestUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.natsConfig;
import static io.memoria.reactive.nats.TestUtils.scheduler;
import static io.memoria.reactive.nats.TestUtils.transformer;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsCommandStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandStreamTest.class.getName());
  private static final String topic = TestUtils.topicName(NatsCommandStreamTest.class);
  private static final int partition = 0;
  private static final CommandStream<AccountCommand> commandStream;
  private static final Data data;

  static {
    try {
      commandStream = new NatsCommandStream<>(natsConfig, AccountCommand.class, transformer, scheduler);
      data = Data.ofUUID();
      NatsUtils.createOrUpdateTopic(natsConfig, topic, 1).map(StreamInfo::toString).forEach(log::info);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void publish() {
    // Given
    var ids = data.createIds(0, TestUtils.MSG_COUNT);
    var commands = ids.map(id -> data.createAccountCmd(id, 500));

    // When
    var pub = commands.flatMap(cmd -> commandStream.pub(topic, partition, cmd));

    // Then
    StepVerifier.create(pub).expectNextCount(TestUtils.MSG_COUNT).verifyComplete();
  }

  @Test
  void subscribe() {
    // When
    var sub = commandStream.sub(topic, partition);

    // Then
    StepVerifier.create(sub).expectNextCount(TestUtils.MSG_COUNT).expectTimeout(TestUtils.TIMEOUT).verify();
  }
}
