package io.memoria.reactive.kafka.eventsourcing.pipeline.partition;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.eventsourcing.pipeline.partition.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.kafka.TestUtils;
import io.memoria.reactive.kafka.eventsourcing.stream.KafkaCommandStream;
import io.memoria.reactive.kafka.eventsourcing.stream.KafkaEventStream;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingInfra;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition.PerformanceScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition.SimpleDebitScenario;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Random;

class ESScenarioTest {
  private static final TextTransformer transformer = new SerializableTransformer();
  // Pipeline
  private final BankingData bankingData = BankingData.ofUUID();
  private final PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline;

  ESScenarioTest() {
    var random = new Random();
    int topicPostfix = random.nextInt(1000);
    // Command
    var commandRoute = new CommandRoute("commands" + topicPostfix, 0);
    var commandStream = new KafkaCommandStream<>(TestUtils.producerConfigs(),
                                                 TestUtils.consumerConfigs(),
                                                 AccountCommand.class,
                                                 transformer);
    // Event
    var eventStream = new KafkaEventStream<>(TestUtils.producerConfigs(),
                                             TestUtils.consumerConfigs(),
                                             AccountEvent.class,
                                             transformer,
                                             Duration.ofMillis(500));
    var eventRoute = new EventRoute("events" + topicPostfix, 0);

    // Pipeline
    this.pipeline = BankingInfra.createPipeline(bankingData.idSupplier,
                                                bankingData.timeSupplier,
                                                commandStream,
                                                commandRoute,
                                                eventStream,
                                                eventRoute);

  }

  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 3, 7, 9, 10, 100, 1000})
  void simpleDebitScenario(int numOfAccounts) {
    // When
    var scenario = new SimpleDebitScenario(bankingData, pipeline, numOfAccounts);

    // Then
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(numOfAccounts * 5L)
                .expectTimeout(Duration.ofMillis(1000))
                .verify();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // When
    var scenario = new PerformanceScenario(bankingData, pipeline, numOfAccounts);
    // Then
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(numOfAccounts * 5L)
                .expectTimeout(Duration.ofMillis(1000))
                .verify();
  }
}
