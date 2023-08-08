package io.memoria.reactive.testsuite.eventsourcing.banking.pipeline;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.atom.testsuite.eventsourcing.banking.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.vavr.collection.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("ClassCanBeRecord")
public class SimpleDebitScenario implements PartitionScenario<AccountCommand, AccountEvent> {
  public static final int INITIAL_BALANCE = 500;
  public static final int DEBIT_AMOUNT = 300;

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
    var createDebitedAcc = bankingData.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = bankingData.createAccountCmd(creditedIds, INITIAL_BALANCE);
    var debitTheAccounts = bankingData.debitCmd(debitedIds.zipWith(creditedIds), DEBIT_AMOUNT);
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
    return Utils.reduce(pipeline.domain.evolver(), events.take(expectedEventsCount()))
                .map(Map::values)
                .flatMapMany(Flux::fromIterable)
                .map(OpenAccount.class::cast)
                .map(this::verify)
                .reduce((a, b) -> a && b);
  }

  boolean verify(OpenAccount acc) {
    if (acc.debitCount() > 0) {
      return acc.balance() == INITIAL_BALANCE - DEBIT_AMOUNT;
    } else if (acc.creditCount() > 0) {
      return acc.balance() == INITIAL_BALANCE + DEBIT_AMOUNT;
    } else {
      return acc.balance() == INITIAL_BALANCE;
    }
  }
}
