package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.stream.Stream;

class ScenarioTest {
  private static final Duration timeout = Duration.ofMillis(1000);

  @ParameterizedTest(name = "Using {0}")
  @MethodSource("dataSource")
  void duplicateCreditScenario(String name, Data data) {
    // Given
    var scenario = new DuplicateCreditScenario(data, 100);
    // When, Then
    StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
  }

  @ParameterizedTest(name = "Using {0}")
  @MethodSource("dataSource")
  void sagaDebitScenario(String name, Data data) {
    // Given
    var scenario = new DebitSagaScenario(data, 100);
    // When, Then
    StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0}")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // Given
    var data = Data.ofUUID("bob");
    var pipeline = data.createMemoryPipeline(new CommandRoute("commands", 0), new EventRoute("events", 0));
    int initBalance = 500;
    int creditBalance = 300;
    var accountIds = data.createIds(0, numOfAccounts);
    var createAccounts = data.createAccountCmd(accountIds, initBalance);
    var creditAccounts = data.creditCmd(accountIds, Id.of("the bank"), creditBalance);
    var commands = Flux.fromIterable(createAccounts).concatWith(Flux.fromIterable(creditAccounts));

    // When
    var now = System.nanoTime();
    StepVerifier.create(pipeline.handle(commands)).expectNextCount(numOfAccounts * 2L).verifyComplete();
    var elapsedTimeMillis = (System.nanoTime() - now) / 1_000_000;

    // Then
    System.out.println(elapsedTimeMillis);
  }

  private static Stream<Arguments> dataSource() {
    var arg1 = Arguments.of("Serial Ids", Data.ofSerial("bob"));
    var arg2 = Arguments.of("TimeUUIDs", Data.ofUUID("bob"));
    return Stream.of(arg1, arg2);
  }
}
