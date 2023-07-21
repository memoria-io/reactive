package io.memoria.reactive.nats.eventsourcing.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.eventsourcing.pipeline.partition.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.Account;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.Data;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.Infra;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.PerformanceScenario;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.SimpleDebitScenario;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.eventsourcing.NatsCommandStream;
import io.memoria.reactive.nats.eventsourcing.NatsEventStream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.Random;

import static io.memoria.reactive.nats.TestUtils.NATS_URL;
import static io.memoria.reactive.nats.TestUtils.createConfig;

class ScenarioTest {
  private static final TextTransformer transformer = new SerializableTransformer();
  // Pipeline
  private final Data data = Data.ofUUID();
  private final PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline;

  ScenarioTest() throws IOException, InterruptedException {
    var random = new Random();
    int topicPostfix = random.nextInt(1000);
    // Command
    var commandRoute = new CommandRoute("commands" + topicPostfix, 0);
    System.out.println(commandRoute);
    var commandConfig = createConfig(commandRoute.name(), commandRoute.totalPartitions());

    var eventRoute = new EventRoute("events" + topicPostfix, 0);
    System.out.println(eventRoute);
    var eventConfig = createConfig(eventRoute.name(), eventRoute.totalPartitions());

    var natsConfig = new NatsConfig(NATS_URL, commandConfig.addAll(eventConfig));

    // Streams
    var commandStream = new NatsCommandStream<>(natsConfig, AccountCommand.class, transformer);
    var eventStream = new NatsEventStream<>(natsConfig, AccountEvent.class, transformer);

    // Pipeline
    this.pipeline = Infra.createPipeline(data.idSupplier,
                                         data.timeSupplier,
                                         commandStream,
                                         commandRoute,
                                         eventStream,
                                         eventRoute);

  }

  @Test
  void simpleDebitScenario() {
    // Given
    int numOfAccounts = 1000;
    // When
    var scenario = new SimpleDebitScenario(data, pipeline, numOfAccounts);
    // Then
    StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // When
    var scenario = new PerformanceScenario(data, pipeline, numOfAccounts);
    // Then
    StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
  }
}
