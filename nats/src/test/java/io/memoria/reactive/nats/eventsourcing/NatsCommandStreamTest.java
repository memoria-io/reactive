package io.memoria.reactive.nats.eventsourcing;

import io.memoria.atom.core.text.SerializableTransformer;
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
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.Random;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class NatsCommandStreamTest {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandStreamTest.class.getName());
  private static final Random r = new Random();
  private static final String topic = "topic" + r.nextInt(1000);
  private static final int partition = 0;
  private static final CommandStream<AccountCommand> commandStream;
  private static final Data data;

  static {
    try {
      var natsConfig = TestUtils.natsConfig();
      commandStream = new NatsCommandStream<>(natsConfig,
                                              AccountCommand.class,
                                              new SerializableTransformer(),
                                              Schedulers.boundedElastic());
      data = Data.ofUUID();
      NatsUtils.createOrUpdateTopic(natsConfig, topic, 1).map(StreamInfo::toString).forEach(log::info);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void publish() {
    // Given
    var ids = data.createIds(0, TestUtils.COUNT);
    var commands = ids.map(id -> data.createAccountCmd(id, 500));

    // When
    var pub = commands.flatMap(cmd -> commandStream.pub(topic, partition, cmd));

    // Then
    StepVerifier.create(pub).expectNextCount(TestUtils.COUNT).verifyComplete();
  }

  @Test
  void subscribe() {
    // When
    var sub = commandStream.sub(topic, partition);

    // Then
    StepVerifier.create(sub).expectNextCount(TestUtils.COUNT).expectTimeout(TestUtils.timeout).verify();
  }
}
