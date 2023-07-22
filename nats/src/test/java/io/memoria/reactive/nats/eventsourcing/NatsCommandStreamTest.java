package io.memoria.reactive.nats.eventsourcing;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.Data;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.Utils;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import static io.memoria.reactive.nats.Utils.NATS_URL;

class NatsCommandStreamTest {
  private static final Duration timeout = Duration.ofMillis(500);
  private static final int COUNT = 10000;
  private static final Random r = new Random();

  private final String topic = "topic" + r.nextInt(1000);
  private final int partition = 0;
  private final CommandStream<AccountCommand> commandStream;
  private final Data data;

  NatsCommandStreamTest() throws IOException, InterruptedException {
    var config = new NatsConfig(NATS_URL, Utils.createConfig(topic, 1));
    this.commandStream = new NatsCommandStream<>(config, AccountCommand.class, new SerializableTransformer());
    this.data = Data.ofUUID();
  }

  @Test
  void publish() {
    // Given
    var ids = data.createIds(0, COUNT);
    var commands = ids.map(id -> data.createAccountCmd(id, 500));
    // When
    var pub = commands.concatMap(cmd -> commandStream.pub(topic, partition, cmd));
    // Then
    StepVerifier.create(pub).expectNextCount(COUNT).verifyComplete();
  }

  @Test
  void subscribe() {
    // Given
    var ids = data.createIds(0, COUNT);
    var commands = ids.map(id -> data.createAccountCmd(id, 500));
    // When
    var pub = commands.concatMap(cmd -> commandStream.pub(topic, partition, cmd));
    var sub = commandStream.sub(topic, partition);

    // Then
    StepVerifier.create(pub).expectNextCount(COUNT).verifyComplete();
    StepVerifier.create(sub).expectNextCount(COUNT).expectTimeout(timeout).verify();
  }
}
