package io.memoria.reactive.eventsourcing.testsuite.banking;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.OpenAccount;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.CreateAndCreditScenario;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.Data;
import io.vavr.Tuple;
import io.vavr.collection.HashMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;

class PipelinesTest {
  @ParameterizedTest(name = "Using {0}")
  @MethodSource("dataSource")
  void simpleCredit(String name, Data data) {
    // Given
    int nAccounts = 100;
    var scenario = new CreateAndCreditScenario(data, nAccounts);
    StepVerifier.create(scenario.verify()).expectNext(true).verifyComplete();
  }

  @ParameterizedTest(name = "Using {0}")
  @MethodSource("dataSource")
  void sagaDebit(String name, Data data) {
    // Given
    var pipeline = data.createMemoryPipeline(new CommandRoute("commands", 0), new EventRoute("events", 0));
    long start = System.nanoTime();
    var nAccounts = 10;
    int initialBalance = 500;
    int debitAmount = 300;

    var allAccountIds = data.createIds(0, nAccounts);
    var debitedIds = allAccountIds.take(nAccounts / 2);
    var creditedIds = allAccountIds.takeRight(nAccounts / 2);

    // Create accounts
    var createAccountCmds = data.createAccountCmd(allAccountIds, initialBalance);
    // Debit half to the other half
    var debitedCreditedMap = HashMap.ofEntries(debitedIds.zipWith(creditedIds, Tuple::of));
    var debitAccountCmds = data.debitCmd(debitedCreditedMap, debitAmount);
    long eventsCount = nAccounts + (debitAccountCmds.size() * 3L);
    System.out.println("Expected events count:" + eventsCount);

    // When
    var allCommands = createAccountCmds.appendAll(debitAccountCmds);
    Flux.fromIterable(allCommands).concatMap(pipeline::pubCommand).subscribe();
    StepVerifier.create(pipeline.handle()).expectNextCount(eventsCount).expectTimeout(Duration.ofMillis(100)).verify();
    //    var seconds = Duration.ofNanos(System.nanoTime() - start).toSeconds();
    //    System.out.printf("Handled %d events per second %n", eventsCount / seconds);

    // Then
    int expectedDebitedBalance = initialBalance - debitAmount;
    int expectedCreditedBalance = initialBalance + debitAmount;
    int expectedTotalBalance = nAccounts * initialBalance;
    var finalStateMap = pipeline.domain.evolver()
                                       .reduce(pipeline.subToEvents().take(eventsCount))
                                       .map(m -> m.mapValues(account -> (OpenAccount) account))
                                       .block();
    Objects.requireNonNull(finalStateMap);
    debitedIds.map(id -> finalStateMap.get(id).get())
              .forEach(account -> Assertions.assertThat(account.balance()).isEqualTo(expectedDebitedBalance));
    creditedIds.map(id -> finalStateMap.get(id).get())
               .forEach(account -> Assertions.assertThat(account.balance()).isEqualTo(expectedCreditedBalance));
    var totalBalance = allAccountIds.map(id -> finalStateMap.get(id).get()).foldLeft(0L, (a, b) -> a + b.balance());
    Assertions.assertThat(totalBalance).isEqualTo(expectedTotalBalance);
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
