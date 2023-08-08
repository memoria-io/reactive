package io.memoria.reactive.testsuite.eventsourcing.banking;

import io.memoria.reactive.testsuite.Utils;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.PerformanceScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.SimpleDebitScenario;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.util.stream.Stream;

class InMemBankingScenarioTest {

  @ParameterizedTest(name = "Using {0} accounts")
  @MethodSource("dataSource")
  void simpleDebitScenario(String name, BankingData bankingData, int numOfAccounts) {
    // Given
    var pipeline = BankingInfra.createMemoryPipeline(bankingData.idSupplier, bankingData.timeSupplier);

    // When
    var scenario = new SimpleDebitScenario(bankingData, pipeline, numOfAccounts);
    StepVerifier.create(scenario.publishCommands()).expectNextCount(numOfAccounts * 3L).verifyComplete();

    // Then
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(numOfAccounts * 5L)
                .expectTimeout(Utils.TIMEOUT)
                .verify();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // Given
    var data = BankingData.ofUUID();
    var pipeline = BankingInfra.createMemoryPipeline(data.idSupplier, data.timeSupplier);
    // When
    var scenario = new PerformanceScenario(data, pipeline, numOfAccounts);
    StepVerifier.create(scenario.publishCommands()).expectNextCount(numOfAccounts * 3L).verifyComplete();
    // Then
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(numOfAccounts * 5L)
                .expectTimeout(Utils.TIMEOUT)
                .verify();
  }

  private static Stream<Arguments> dataSource() {
    var arg1 = Arguments.of("Serial Ids", BankingData.ofSerial(), 100);
    var arg2 = Arguments.of("TimeUUIDs", BankingData.ofUUID(), 100);
    return Stream.of(arg1, arg2);
  }
}
