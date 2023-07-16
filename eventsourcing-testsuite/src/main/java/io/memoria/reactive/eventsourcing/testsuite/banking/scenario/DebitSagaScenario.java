package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.OpenAccount;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class DebitSagaScenario implements Scenario {
  private static final Duration timeout = Duration.ofMillis(50);
  private final Data data;
  private final int initialBalance = 500;
  private final int debitAmount = 300;
  private final List<Id> accountsList;
  private final List<Id> debitedList;
  private final Set<Id> debitedSet;
  private final List<Id> creditedIds;

  public DebitSagaScenario(Data data, int numOfAccounts) {
    this.data = data;
    accountsList = data.createIds(0, numOfAccounts);
    debitedList = accountsList.take(numOfAccounts / 2);
    debitedSet = debitedList.toSet();
    creditedIds = accountsList.takeRight(numOfAccounts / 2);
  }

  @Override
  public Mono<Boolean> verify() {
    // Given
    var createAccountCmds = data.createAccountCmd(accountsList, initialBalance);
    var debitAccountCmds = data.debitCmd(debitedList, creditedIds, debitAmount);
    var commandsList = createAccountCmds.appendAll(debitAccountCmds);

    // When
    var pipeline = data.createMemoryPipeline(new CommandRoute("commands", 0), new EventRoute("events", 0));
    Flux.fromIterable(commandsList).concatMap(pipeline::pubCommand).subscribe();

    // Then
    var events = pipeline.handle().timeout(timeout).onErrorComplete(TimeoutException.class);
    return data.domain.evolver()
                      .reduce(events)
                      .map(Map::values)
                      .flatMapMany(Flux::fromIterable)
                      .map(acc -> (OpenAccount) acc)
                      .map(this::verify)
                      .reduce((a, b) -> a && b);
  }

  boolean verify(OpenAccount acc) {
    if (debitedSet.contains(acc.accountId())) {
      return hasExpectedBalance(acc, initialBalance - debitAmount);
    } else {
      return hasExpectedBalance(acc, initialBalance + debitAmount);
    }
  }

  private static boolean hasExpectedBalance(OpenAccount acc, int expected) {
    if (acc.balance() == expected) {
      // System.out.println("success:" + acc);
      return true;
    } else {
      // System.out.println("fail:" + acc);
      var msg = "Account %s balance is %d not %d".formatted(acc.accountId(), acc.balance(), expected);
      throw new IllegalStateException(msg);
    }
  }
}
