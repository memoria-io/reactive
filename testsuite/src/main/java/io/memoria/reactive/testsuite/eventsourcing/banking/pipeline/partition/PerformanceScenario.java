package io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition;

import io.memoria.reactive.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountCreated;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.Credited;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.DebitConfirmed;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.Debited;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class PerformanceScenario implements PartitionScenario<Account, AccountCommand, AccountEvent> {
  private static final int initialBalance = 500;
  private static final int debitAmount = 300;

  private final BankingData bankingData;
  private final PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline;
  private final int numOfAccounts;

  public PerformanceScenario(BankingData bankingData,
                             PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline,
                             int numOfAccounts) {
    this.bankingData = bankingData;
    this.pipeline = pipeline;
    this.numOfAccounts = numOfAccounts;
  }

  @Override
  public int expectedCommandsCount() {
    return numOfAccounts * 3;
  }

  @Override
  public int expectedEventsCount() {
    return numOfAccounts * 5;
  }

  @Override
  public Flux<AccountCommand> publishCommands() {
    var debitedIds = bankingData.createIds(0, numOfAccounts).map(StateId::of);
    var creditedIds = bankingData.createIds(numOfAccounts, numOfAccounts).map(StateId::of);
    var createDebitedAcc = bankingData.createAccountCmd(debitedIds, initialBalance);
    var createCreditedAcc = bankingData.createAccountCmd(creditedIds, initialBalance);
    var debitTheAccounts = bankingData.debitCmd(debitedIds.zipWith(creditedIds), debitAmount);
    var commands = createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);

    return commands.concatMap(pipeline::pubCommand);
  }

  @Override
  public Flux<AccountEvent> handleCommands() {
    return pipeline.handle();
  }

  @Override
  public Mono<Boolean> verify() {
    var startTime = System.currentTimeMillis();
    return null;
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