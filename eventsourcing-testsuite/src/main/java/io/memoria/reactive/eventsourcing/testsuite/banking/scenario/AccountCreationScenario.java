package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.OpenAccount;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class AccountCreationScenario implements Scenario {
  private final Data data;
  private final int numOfAccounts;
  private final int initialBalance = 500;

  public AccountCreationScenario(Data data, int numOfAccounts) {
    this.data = data;
    this.numOfAccounts = numOfAccounts;
  }

  @Override
  public Mono<Boolean> verify() {
    // Given
    var accountIds = data.createIds(0, numOfAccounts);
    var commands = data.createAccountCmd(accountIds, initialBalance);
    var pipeline = data.createMemoryPipeline(new CommandRoute("commands", 0), new EventRoute("events", 0));

    // Then
    return data.domain.evolver()
                      .reduce(pipeline.handle(Flux.fromIterable(commands)))
                      .flatMapMany(accounts -> Flux.fromIterable(accounts.values()))
                      .map(acc -> (OpenAccount) acc)
                      .map(this::hasRightBalance)
                      .map(this::hasRightName)
                      .reduce((a, b) -> a && b);
  }

  OpenAccount hasRightBalance(OpenAccount acc) {
    if (acc.balance() == initialBalance) {
      return acc;
    } else {
      throw new IllegalStateException("Account balance %d != %d".formatted(acc.balance(), initialBalance));
    }
  }

  boolean hasRightName(OpenAccount acc) {
    var expected = data.createName(0);
    if (acc.name().equals(expected)) {
      return true;
    } else {
      throw new IllegalStateException("Account name %s != %s".formatted(acc.name(), expected));
    }
  }
}
