package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.OpenAccount;
import io.vavr.collection.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CreateAndCreditScenario implements Scenario {
  private static final Id atmId = Id.of("the bank atm");

  private final Data data;
  private final int numOfAccounts;
  private final int initBalance = 500;
  private final int creditBalance = 300;

  public CreateAndCreditScenario(Data data, int numOfAccounts) {
    this.data = data;
    this.numOfAccounts = numOfAccounts;
  }

  @Override
  public Mono<Boolean> verify() {
    // Given
    var scenario = new CreateAndCreditScenario(data, numOfAccounts);
    var commands = scenario.create();
    var pipeline = data.createMemoryPipeline(new CommandRoute("commands", 0), new EventRoute("events", 0));

    // Then
    return data.domain.evolver()
                      .reduce(pipeline.handle(Flux.fromIterable(commands)))
                      .flatMapMany(accounts -> Flux.fromIterable(accounts.values()))
                      .map(acc -> (OpenAccount) acc)
                      .map(this::hasRightBalance)
                      .reduce((a, b) -> a && b);
  }

  List<AccountCommand> create() {
    var accountIds = data.createIds(0, numOfAccounts);
    var createAccounts = data.createAccountCmd(accountIds, initBalance);
    var creditAccounts = data.creditCmd(accountIds, atmId, creditBalance);
    return createAccounts.appendAll(creditAccounts).appendAll(createAccounts).appendAll(creditAccounts);
  }

  boolean hasRightBalance(OpenAccount acc) {
    int finalBalance = initBalance + creditBalance;
    if (acc.balance() == finalBalance) {
      return true;
    } else {
      throw new IllegalStateException("Account balance %d != %d".formatted(acc.balance(), finalBalance));
    }
  }
}
