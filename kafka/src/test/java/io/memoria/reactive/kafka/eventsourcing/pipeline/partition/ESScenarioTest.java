package io.memoria.reactive.kafka.eventsourcing.pipeline.partition;

import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition.PerformanceScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition.SimpleDebitScenario;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.time.Duration;

import static io.memoria.reactive.kafka.TestUtils.createPipeline;
import static io.memoria.reactive.kafka.TestUtils.data;

class ESScenarioTest {

  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 3, 7, 9, 10, 100, 1000})
  void simpleDebitScenario(int numOfAccounts) {
    // When
    var scenario = new SimpleDebitScenario(data, createPipeline(), numOfAccounts);

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
    var scenario = new PerformanceScenario(data, createPipeline(), numOfAccounts);
    // Then
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(scenario.expectedEventsCount())
                .expectTimeout(Duration.ofMillis(1000))
                .verify();
  }
}
