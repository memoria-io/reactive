package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.util.stream.Stream;

class ScenarioTest {

  @ParameterizedTest(name = "Using {0} accounts")
  @MethodSource("dataSource")
  void simpleDebitScenario(String name, Data data, int numOfAccounts) {
    // Given
    var pipeline = Infra.createMemoryPipeline(data.idSupplier, data.timeSupplier);
    // When
    var scenario = new SimpleDebitScenario(data, pipeline, numOfAccounts);
    // Then
    StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // Given
    var data = Data.ofUUID();
    var pipeline = Infra.createMemoryPipeline(data.idSupplier, data.timeSupplier);
    // When
    var scenario = new PerformanceScenario(data, pipeline, numOfAccounts);
    // Then
    StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
  }

  private static Stream<Arguments> dataSource() {
    var arg1 = Arguments.of("Serial Ids", Data.ofSerial(), 100);
    var arg2 = Arguments.of("TimeUUIDs", Data.ofUUID(), 100);
    return Stream.of(arg1, arg2);
//    return Stream.of(arg2);
  }
}
