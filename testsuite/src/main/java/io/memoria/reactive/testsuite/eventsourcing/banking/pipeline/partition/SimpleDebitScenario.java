package io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition;

import io.memoria.reactive.eventsourcing.PipelineUtils;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.OpenAccount;
import io.vavr.collection.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("ClassCanBeRecord")
public class SimpleDebitScenario implements PartitionScenario<Account, AccountCommand, AccountEvent> {
  public static final int initialBalance = 500;
  public static final int debitAmount = 300;

  public final BankingData bankingData;
  public final PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline;
  public final int numOfAccounts;

  public SimpleDebitScenario(BankingData bankingData,
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
    var events = pipeline.eventStream.sub(pipeline.eventRoute.topicName(), pipeline.eventRoute.partition());
    return PipelineUtils.reduce(pipeline.domain.evolver(), events.take(expectedEventsCount()))
                        .map(Map::values)
                        .flatMapMany(Flux::fromIterable)
                        .map(acc -> (OpenAccount) acc)
                        .map(this::verify)
                        .reduce((a, b) -> a && b);
  }

  boolean verify(OpenAccount acc) {
    if (acc.debitCount() > 0) {
      return hasExpectedBalance(acc, initialBalance - debitAmount);
    } else if (acc.creditCount() > 0) {
      return hasExpectedBalance(acc, initialBalance + debitAmount);
    } else {
      return hasExpectedBalance(acc, initialBalance);
    }
  }

  private static boolean hasExpectedBalance(OpenAccount acc, int expected) {
    if (acc.balance() == expected) {
      //      System.out.println("success:" + acc);
      return true;
    } else {
      //      System.out.println("fail:" + acc);
      var msg = "Account %s balance is %d not %d".formatted(acc.accountId(), acc.balance(), expected);
      throw new IllegalStateException(msg);
    }
  }
}
