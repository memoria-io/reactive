package io.memoria.reactive.nats.eventsourcing.pipeline.partition;

import io.memoria.reactive.nats.TestUtils;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition.PerformanceScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition.SimpleDebitScenario;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import static io.memoria.reactive.testsuite.TestsuiteUtils.TIMEOUT;

class ESScenarioTest {

  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {0, 1, 3, 7, 9, 10, 98, 998, 999, 1000, 1111, 2222, 3333})
  void simpleDebitScenario(int numOfAccounts) {
    // When
    var scenario = new SimpleDebitScenario(TestUtils.data, TestUtils.createPipeline(), numOfAccounts);
    StepVerifier.create(scenario.publishCommands().doOnNext(System.out::println))
                .expectNextCount(scenario.expectedCommandsCount())
                .verifyComplete();

    // Then
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(scenario.expectedEventsCount())
                .expectTimeout(TIMEOUT)
                .verify();
    System.out.println(System.currentTimeMillis() - now);
    //    StepVerifier.create(scenario.verify(scenario.handle()))
    //                .expectNextCount(numOfAccounts * 5L)
    //                .expectTimeout(timeout)
    //                .verify();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // When
    var scenario = new PerformanceScenario(TestUtils.data, TestUtils.createPipeline(), numOfAccounts);
    StepVerifier.create(scenario.publishCommands().doOnNext(System.out::println))
                .expectNextCount(scenario.expectedCommandsCount())
                .verifyComplete();

    // Then
    StepVerifier.create(scenario.handleCommands()).expectNextCount(numOfAccounts * 5L).expectTimeout(TIMEOUT).verify();
  }
}
