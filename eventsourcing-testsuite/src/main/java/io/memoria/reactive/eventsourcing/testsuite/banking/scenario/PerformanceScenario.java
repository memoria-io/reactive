package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountCreated;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.Credited;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.DebitConfirmed;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.Debited;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.Account;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class PerformanceScenario implements Scenario {
  private static final Duration timeout = Duration.ofMillis(50);
  private static final int initialBalance = 500;
  private static final int debitAmount = 300;

  private final Data data;
  private final PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline;
  private final int numOfAccounts;

  public PerformanceScenario(Data data,
                             PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline,
                             int numOfAccounts) {
    this.data = data;
    this.pipeline = pipeline;
    this.numOfAccounts = numOfAccounts;
  }

  @Override
  public Mono<Boolean> verify() {
    var debitedIds = data.createIds(0, numOfAccounts);
    var creditedIds = data.createIds(numOfAccounts, numOfAccounts * 2);

    // Given
    var createDebitedAcc = data.createAccountCmd(debitedIds, initialBalance);
    var createCreditedAcc = data.createAccountCmd(creditedIds, initialBalance);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), debitAmount);
    var commands = createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);

    // When
    commands.concatMap(pipeline::pubCommand).subscribe();

    // Then
    var startTime = System.currentTimeMillis();
    return pipeline.handle()
                   .take(numOfAccounts * 6L)
                   .count()
                   .doOnNext(count -> printf(startTime, count))
                   .map(i -> true);
    //                   .timeout(timeout)
    //                   .onErrorComplete(TimeoutException.class)
    //                   .all(PerformanceScenario::isTypeOf);
  }

  private static void printf(long start, Long i) {
    System.out.printf("Processed %d events in  %d millis %n", i, System.currentTimeMillis() - start);
  }

  private static boolean isTypeOf(AccountEvent acc) {
    if (acc instanceof AccountCreated
        || acc instanceof Debited
        || acc instanceof Credited
        || acc instanceof DebitConfirmed) {
      return true;
    } else {
      throw new IllegalStateException("Unknown event %s".formatted(acc.getClass().getSimpleName()));
    }
  }
}
