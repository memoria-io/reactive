package io.memoria.reactive.kafka;

import io.memoria.reactive.testsuite.Utils;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.PerformanceScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.SimpleDebitScenario;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import static io.memoria.reactive.kafka.TestUtils.DATA;
import static io.memoria.reactive.kafka.TestUtils.createPipeline;
import static io.memoria.reactive.testsuite.Utils.MSG_COUNT;
import static io.memoria.reactive.testsuite.Utils.TIMEOUT;

class ESScenarioTest {

  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = MSG_COUNT)
  void simpleDebitScenario(int numOfAccounts) {
    // When
    var scenario = new SimpleDebitScenario(DATA, createPipeline(), numOfAccounts);
    StepVerifier.create(scenario.publishCommands()).expectNextCount(scenario.expectedCommandsCount()).verifyComplete();

    // Then
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.handleCommands()).expectNextCount(numOfAccounts * 5L).expectTimeout(TIMEOUT).verify();
    Utils.printRates("scenario", now);
    if (numOfAccounts > 0) {
      //      StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
    }
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // When
    var scenario = new PerformanceScenario(DATA, createPipeline(), numOfAccounts);
    StepVerifier.create(scenario.publishCommands()).expectNextCount(scenario.expectedCommandsCount()).verifyComplete();

    // Then
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(scenario.expectedEventsCount())
                .expectTimeout(TIMEOUT)
                .verify();
    Utils.printRates("Performance scenario", now);
  }
}
