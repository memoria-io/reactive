package io.memoria.reactive.eventsourcing.usecase.banking;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.stream.ESMsgStream;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.CommandPipeline;
import io.memoria.reactive.eventsourcing.pipeline.PipelineRoute;
import io.memoria.reactive.eventsourcing.pipeline.PipelineStateRepo;
import io.memoria.reactive.eventsourcing.usecase.banking.command.AccountCommand;
import io.memoria.reactive.eventsourcing.usecase.banking.event.AccountEvent;
import io.memoria.reactive.eventsourcing.usecase.banking.state.Account;
import io.memoria.reactive.eventsourcing.usecase.banking.state.OpenAccount;
import io.vavr.collection.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

class PipelinesTest {
  private static final TextTransformer transformer = new SerializableTransformer();
  private static final PipelineRoute PIPELINE_ROUTE = new PipelineRoute("commands", 0, 1, "events", 0);
  private final ESMsgStream esStream = ESMsgStream.inMemory();
  private final PipelineStateRepo<AccountEvent> pipelineState = PipelineStateRepo.inMemory(PIPELINE_ROUTE);
  private final CommandPipeline<Account, AccountCommand, AccountEvent> pipeline = createPipeline();
  private static final int nAccounts = 10;
  private static final int initialBalance = 500;
  private static final int creditBalance = 300;
  private static final int debitBalance = 300;

  @Disabled("Disabled and is enabled for debugging purposes only")
  @ParameterizedTest
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // Given
    var createAccounts = Data.createAccounts(numOfAccounts, initialBalance);
    var creditAccounts = Data.credit(numOfAccounts, creditBalance);
    creditAccounts = createAccounts.appendAll(creditAccounts); // Handling duplicates (at least once messaging)
    var commands = Flux.fromIterable(createAccounts).concatWith(Flux.fromIterable(creditAccounts));

    // When
    var now = System.nanoTime();
    StepVerifier.create(pipeline.handle(commands)).expectNextCount(numOfAccounts * 2L).verifyComplete();
    var elapsedTimeMillis = (System.nanoTime() - now) / 1_000_000;

    // Then
    System.out.println(elapsedTimeMillis);
  }

  @Test
  void simpleCreditWithVerification() {
    // Given
    int endBalance = initialBalance + creditBalance;
    var createAccounts = Data.createAccounts(nAccounts, initialBalance);
    var creditAccounts = Data.credit(nAccounts, creditBalance);
    creditAccounts = createAccounts.appendAll(creditAccounts); // Handling duplicates (at least once messaging)
    var commands = Flux.fromIterable(createAccounts).concatWith(Flux.fromIterable(creditAccounts));

    // When
    StepVerifier.create(pipeline.handle(commands)).expectNextCount(nAccounts * 2).verifyComplete();

    // Then
    List.range(0, nAccounts).map(this::account).forEach(id -> verifyBalance(id, endBalance));
  }

  @Test
  void sagaDebitWithVerification() {
    // Given
    int expectedDebitedAccountsBalance = initialBalance - debitBalance;
    int expectedCreditedAccountsBalance = initialBalance + debitBalance;
    int expectedTotalBalance = nAccounts * initialBalance;

    var createAccounts = Data.createAccounts(nAccounts, initialBalance);
    var debitAccounts = Data.debit(nAccounts, debitBalance);
    var commands = Flux.fromIterable(createAccounts).concatWith(Flux.fromIterable(debitAccounts));
    commands.flatMap(pipeline::pubCommand).subscribe();

    // When
    StepVerifier.create(pipeline.handle()).expectNextCount(25).expectTimeout(Duration.ofMillis(1000)).verify();

    // Then
    List.range(0, nAccounts / 2).map(this::account).forEach(id -> verifyBalance(id, expectedDebitedAccountsBalance));

    List.range(nAccounts / 2, nAccounts)
        .map(this::account)
        .forEach(id -> verifyBalance(id, expectedCreditedAccountsBalance));

    var totalBalance = List.range(0, nAccounts)
                           .map(this::account)
                           .map(Mono::block)
                           .foldLeft(0, (a, b) -> a + b.balance());
    Assertions.assertThat(totalBalance).isEqualTo(expectedTotalBalance);
  }

  private void verifyBalance(Mono<OpenAccount> account, int endBalance) {
    StepVerifier.create(account).expectNextMatches(acc -> acc.balance() == endBalance).verifyComplete();
  }

  private Mono<OpenAccount> account(int accountId) {
    var stateEvents = pipeline.subToEvents();
    var stateId = Data.createAccountId(accountId);
    return pipeline.domain.evolver().reduce(stateId, stateEvents, 2).map(acc -> (OpenAccount) acc);
  }

  private CommandPipeline<Account, AccountCommand, AccountEvent> createPipeline() {
    return new CommandPipeline<>(stateDomain(), PIPELINE_ROUTE, esStream, pipelineState, transformer);
  }

  private static Domain<Account, AccountCommand, AccountEvent> stateDomain() {
    return new Domain<>(Account.class,
                        AccountCommand.class,
                        AccountEvent.class,
                        new AccountDecider(),
                        new AccountEvolver(),
                        new AccountSaga());
  }
}
