package io.memoria.reactive.kafka.eventsourcing.testsuite;

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
import io.memoria.reactive.kafka.KafkaUtils;
import io.memoria.reactive.kafka.TestUtils;
import io.memoria.reactive.kafka.eventsourcing.KafkaCommandStream;
import io.memoria.reactive.kafka.eventsourcing.KafkaEventStream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Random;

class ScenarioTest {
  private static final TextTransformer transformer = new SerializableTransformer();
  // Pipeline
  private final Data data = Data.ofUUID();
  private final PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline;

  ScenarioTest() {
    var sender = KafkaUtils.createSender(TestUtils.producerConfigs());
    var random = new Random();
    int topicPostfix = random.nextInt(1000);
    // Command
    var commandRoute = new CommandRoute("commands" + topicPostfix, 0);
    var commandStream = new KafkaCommandStream<>(TestUtils.producerConfigs(),
                                                 TestUtils.consumerConfigs(),
                                                 AccountCommand.class,
                                                 transformer,
                                                 sender);
    // Event
    var eventStream = new KafkaEventStream<>(TestUtils.producerConfigs(),
                                             TestUtils.consumerConfigs(),
                                             AccountEvent.class,
                                             transformer,
                                             sender,
                                             Duration.ofMillis(500));
    var eventRoute = new EventRoute("events" + topicPostfix, 0);

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
    int numOfAccounts = 100;
    // When
    var scenario = new SimpleDebitScenario(Duration.ofSeconds(1), data, pipeline, numOfAccounts);
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
