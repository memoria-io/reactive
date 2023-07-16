package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.Account;
import io.vavr.collection.List;
import reactor.core.publisher.Flux;

public class SimpleCreditScenario {
  // Given
  public final Id atmId = Id.of("the bank atm");
  public final int numOfAccounts;
  public final int initBalance;
  public final int creditBalance;
  // Expected
  public final int expectedCommands;
  public final int expectedEvents;

  public SimpleCreditScenario(int numOfAccounts, int initBalance, int creditBalance) {
    this.numOfAccounts = numOfAccounts;
    this.initBalance = initBalance;
    this.creditBalance = creditBalance;
    this.expectedCommands = numOfAccounts * 2;
    this.expectedEvents = numOfAccounts * 2;

  }

  /**
   * @return Commands of Simple crediting scenario with duplicates to check idempotence
   */
  public List<AccountCommand> commands(Data data) {
    // Given
    var accountIds = data.createIds(0, numOfAccounts);
    var createAccounts = data.createAccountCmd(accountIds, initBalance);
    var creditAccounts = data.creditCmd(accountIds, atmId, creditBalance);

    return createAccounts.appendAll(creditAccounts).appendAll(createAccounts).appendAll(creditAccounts);
    //    StepVerifier.create(pipeline.handle(Flux.fromIterable(commands))).expectNextCount(commands.size()).verifyComplete();
    //    var finalStateMap = pipeline.domain.evolver().reduce(pipeline.subToEvents().take(nAccounts * 2)).block();
    //
    //    // Then
    //    Objects.requireNonNull(finalStateMap);
    //    finalStateMap.forEach((k, v) -> Assertions.assertThat(((OpenAccount) v).balance())
    //                                              .isEqualTo(initBalance + creditBalance));
  }

  public boolean verifyBalance(Domain<Account, AccountCommand, AccountEvent> domain, Flux<AccountEvent> events) {
    domain.evolver().reduce(events).map(map -> map.)
  }

}
