package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.OpenAccount;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DuplicateCreditScenario implements Scenario {
  private static final Id atmId = Id.of("the bank atm");

  private final Data data;
  private final int numOfAccounts;
  private final int initialBalance = 500;
  private final int creditAmount = 300;

  public DuplicateCreditScenario(Data data, int numOfAccounts) {
    this.data = data;
    this.numOfAccounts = numOfAccounts;
  }

  @Override
  public Mono<Boolean> verify() {
    // Given
    var accountIds = data.createIds(0, numOfAccounts);
    var createAccounts = data.createAccountCmd(accountIds, initialBalance);
    var creditAccounts = data.creditCmd(accountIds, atmId, creditAmount);
    var commandsList = createAccounts.appendAll(creditAccounts).appendAll(createAccounts).appendAll(creditAccounts);
    var commands = Flux.fromIterable(commandsList);
    var pipeline = data.createMemoryPipeline(new CommandRoute("commands", 0), new EventRoute("events", 0));

    // Then
    return data.domain.evolver()
                      .reduce(pipeline.handle(commands))
                      .flatMapMany(accounts -> Flux.fromIterable(accounts.values()))
                      .map(acc -> (OpenAccount) acc)
                      .map(this::hasRightBalance)
                      .reduce((a, b) -> a && b);
  }

  boolean hasRightBalance(OpenAccount acc) {
    int finalBalance = initialBalance + creditAmount;
    if (acc.balance() == finalBalance) {
      return true;
    } else {
      throw new IllegalStateException("Account balance %d != %d".formatted(acc.balance(), finalBalance));
    }
  }
}
