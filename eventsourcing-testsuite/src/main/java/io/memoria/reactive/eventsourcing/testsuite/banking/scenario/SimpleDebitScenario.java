package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.reactive.eventsourcing.pipeline.CommandPipeline;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.Account;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.OpenAccount;
import io.vavr.collection.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class SimpleDebitScenario implements Scenario {
  private static final Duration timeout = Duration.ofMillis(50);
  private static final int initialBalance = 500;
  private static final int debitAmount = 300;

  private final Data data;
  private final CommandPipeline<Account, AccountCommand, AccountEvent> pipeline;
  private final int debited;
  private final int credited;

  public SimpleDebitScenario(Data data,
                             CommandPipeline<Account, AccountCommand, AccountEvent> pipeline,
                             int debited,
                             int credited) {
    this.data = data;
    this.pipeline = pipeline;
    this.debited = debited;
    this.credited = credited;
  }

  @Override
  public Mono<Boolean> verify() {
    var debitedIds = data.createIds(0, debited);
    var creditedIds = data.createIds(debited, debited + credited);

    // Given
    var createDebitedAcc = data.createAccountCmd(debitedIds, initialBalance);
    var createCreditedAcc = data.createAccountCmd(creditedIds, initialBalance);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), debitAmount);
    var commands = createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);

    // When
    commands.concatMap(pipeline::pubCommand).subscribe();

    // Then
    var events = pipeline.handle().timeout(timeout).onErrorComplete(TimeoutException.class);
    return pipeline.domain.evolver()
                          .reduce(events)
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
      System.out.println("success:" + acc);
      return true;
    } else {
      System.out.println("fail:" + acc);
      var msg = "Account %s balance is %d not %d".formatted(acc.accountId(), acc.balance(), expected);
      throw new IllegalStateException(msg);
    }
  }
}
