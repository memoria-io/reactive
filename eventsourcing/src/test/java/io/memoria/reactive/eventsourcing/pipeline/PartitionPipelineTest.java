package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.command.CommandId;
import io.memoria.atom.eventsourcing.command.CommandMeta;
import io.memoria.atom.eventsourcing.event.EventId;
import io.memoria.atom.eventsourcing.event.EventMeta;
import io.memoria.atom.eventsourcing.state.StateId;
import io.memoria.atom.eventsourcing.exceptions.InvalidEvolution;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.command.CreateAccount;
import io.memoria.atom.testsuite.eventsourcing.command.Credit;
import io.memoria.atom.testsuite.eventsourcing.event.AccountCreated;
import io.memoria.atom.testsuite.eventsourcing.event.Debited;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

class PartitionPipelineTest {
  private static final Duration timeout = Duration.ofMillis(200);
  private static final int NUM_OF_ACCOUNTS = 10;
  private static final int NUM_OF_EXPECTED_EVENTS = expectedEventsCount(NUM_OF_ACCOUNTS);

  private final Infra infra = new Infra();
  private final Data data = Data.ofSerial();

  @Test
  void happyPath() {
    // Given
    var pipeline = createSimplePipeline();

    // When
    createAccounts(NUM_OF_ACCOUNTS).concatWith(debitAccounts(NUM_OF_ACCOUNTS))
                                   .concatMap(pipeline::publishCommand)
                                   .subscribe();

    // Then
    StepVerifier.create(pipeline.start())
                .expectNextCount(NUM_OF_EXPECTED_EVENTS)
                .expectTimeout(Duration.ofMillis(100))
                .verify();
  }

  @Test
  @DisplayName("Initialize should pick up last state")
  void initialize() {
    // Given
    var pipeline = createSimplePipeline();

    // When creating accounts
    createAccounts(NUM_OF_ACCOUNTS).concatMap(pipeline::publishCommand).subscribe();

    // Then
    StepVerifier.create(pipeline.start())
                .expectNextCount(NUM_OF_ACCOUNTS)
                .expectTimeout(Duration.ofMillis(100))
                .verify();

    // When old pipeline died
    pipeline = createSimplePipeline();
    debitAccounts(NUM_OF_ACCOUNTS).concatMap(pipeline::publishCommand).subscribe();

    // Then new pipeline should pick up last state
    StepVerifier.create(pipeline.start())
                .expectNextCount(NUM_OF_EXPECTED_EVENTS)
                .expectTimeout(Duration.ofMillis(100))
                .verify();
  }

  @Test
  @DisplayName("Should handle commands with assigned partition")
  void shouldNotRedirect() {
    // Given
    var commandRoute = new CommandRoute("commands", 0, 2);
    var eventRoute = new EventRoute("events");
    var pipeline = infra.inMemoryPipeline(data.domain(), commandRoute, eventRoute);

    // When
    var cmd = data.createAccountCmd(StateId.of(1), 300);
    var isInPartition = cmd.meta().isInPartition(commandRoute.partition(), commandRoute.totalPartitions());

    // Then
    Assertions.assertThat(isInPartition).isTrue();
    StepVerifier.create(pipeline.redirectIfNeeded(cmd)).expectNext(cmd).verifyComplete();
  }

  @Test
  @DisplayName("Should redirect commands with unassigned partition ")
  void shouldRedirect() {
    // Given
    var commandRoute = new CommandRoute("commands", 0, 2);
    var eventRoute = new EventRoute("events");
    var pipeline = infra.inMemoryPipeline(data.domain(), commandRoute, eventRoute);
    // When
    var cmd = data.createAccountCmd(StateId.of(0), 300);
    var isInPartition = cmd.meta().isInPartition(commandRoute.partition(), commandRoute.totalPartitions());
    // Then
    Assertions.assertThat(isInPartition).isFalse();
    StepVerifier.create(pipeline.redirectIfNeeded(cmd)).expectComplete().verify();
  }

  @Test
  void invalidCreationEvolution() {
    // Given
    var pipeline = createSimplePipeline();
    int VERSION_ZERO = 0;
    var meta = new EventMeta(EventId.of(0), VERSION_ZERO, StateId.of(0), CommandId.of(0));
    var accountCreated = new AccountCreated(meta, "alice", 300);
    pipeline.evolve(accountCreated);
    // When
    int VERSION_THREE = 3;
    var meta2 = new EventMeta(EventId.of(1), VERSION_THREE, StateId.of(1), CommandId.of(1));
    var anotherAccountCreated = new AccountCreated(meta2, "bob", 300);
    Assertions.assertThatThrownBy(() -> pipeline.evolve(anotherAccountCreated)).isInstanceOf(InvalidEvolution.class);
  }

  @Test
  void saga() {

  }

  @Test
  void atLeastOnceEvents() {
    // Given
    var pipeline = createSimplePipeline();
    int numOfAccounts = 2;
    int expectedEventsCount = expectedEventsCount(numOfAccounts);
    createAccounts(numOfAccounts).concatWith(debitAccounts(numOfAccounts))
                                 .concatMap(pipeline::publishCommand)
                                 .subscribe();
    StepVerifier.create(pipeline.start()).expectNextCount(expectedEventsCount).expectTimeout(timeout).verify();
    var lastEvent = pipeline.subscribeToEvents().take(expectedEventsCount).last().block();

    // When
    pipeline.publishEvent(lastEvent).block();
    pipeline.publishEvent(lastEvent).block();

    // Event is still ignored
    var restartedPipeline = createSimplePipeline();
    StepVerifier.create(restartedPipeline.start())
                .expectNextCount(expectedEventsCount)
                .expectTimeout(timeout)
                .verify();
  }

  @Test
  @DisplayName("Command should be dropped when it's already handled")
  void isHandledCommand() {
    // Given
    var pipeline = createSimplePipeline();

    // When
    var createAccounts = requireNonNull(createAccounts(NUM_OF_ACCOUNTS).collectList().block());
    Flux.fromIterable(createAccounts).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.start()).expectNextCount(NUM_OF_ACCOUNTS).expectTimeout(timeout).verify();

    // Then
    Assertions.assertThat(pipeline.isDuplicateCommand(createAccounts.getFirst())).isTrue();
  }

  @Test
  void duplicateSagaCommand() {
    // Given
    var pipeline = createSimplePipeline();

    // When
    createAccounts(NUM_OF_ACCOUNTS).concatWith(debitAccounts(NUM_OF_ACCOUNTS))
                                   .concatMap(pipeline::publishCommand)
                                   .subscribe();
    StepVerifier.create(pipeline.start()).expectNextCount(NUM_OF_EXPECTED_EVENTS).expectTimeout(timeout).verify();

    // Then
    var creditCmd = pipeline.subscribeToCommands()
                            .filter(cmd -> cmd instanceof Credit)
                            .map(command -> (Credit) command)
                            .blockFirst();
    assert creditCmd != null;
    var newMeta = new CommandMeta(CommandId.of(UUID.randomUUID()),
                                  creditCmd.meta().stateId(),
                                  System.currentTimeMillis(),
                                  creditCmd.meta().sagaSource());
    var duplicateSagaCommand = new Credit(newMeta, creditCmd.debitedAcc(), 100);
    Assertions.assertThat(pipeline.isDuplicateSagaCommand(duplicateSagaCommand)).isTrue();
  }

  @Test
  @DisplayName("Command is not valid for state")
  void wrongCommand() {
    // Given
    var pipeline = createSimplePipeline();

    // when
    pipeline.publishCommand(debitAccounts(NUM_OF_ACCOUNTS).blockFirst()).subscribe();

    // Then
    StepVerifier.create(pipeline.start()).expectError(InvalidEvolution.class).verify();
  }

  @Test
  void invalidEventVersion() {
    // Given
    var pipeline = createSimplePipeline();
    StateId bobState = StateId.of(0);
    // Create Account
    var commandMeta = new CommandMeta(CommandId.of(0), bobState);
    String bob = "bob";
    StepVerifier.create(pipeline.handle(Flux.just(new CreateAccount(commandMeta, bob, 300))))
                .expectNextCount(1)
                .verifyComplete();

    // When
    int WRONG_VERSION = 10;
    var eventMeta = new EventMeta(EventId.of(1), WRONG_VERSION, bobState, CommandId.of(2));
    var invalidEventVersion = new Debited(eventMeta, bobState, 200);

    // Then
    Assertions.assertThatThrownBy(() -> pipeline.evolveWithState(invalidEventVersion))
              .isInstanceOf(InvalidEvolution.class);
  }

  private PartitionPipeline createSimplePipeline() {
    return infra.inMemoryPipeline(data.domain(), new CommandRoute("commands"), new EventRoute("events"));
  }

  private Flux<AccountCommand> createAccounts(int n) {
    var createDebitedAcc = data.createAccountCmd(debitedIds(n), 500);
    var createCreditedAcc = data.createAccountCmd(creditedIds(n), 500);
    return createDebitedAcc.concatWith(createCreditedAcc);
  }

  private Flux<AccountCommand> debitAccounts(int n) {
    var map = debitedIds(n).zipWith(creditedIds(n));
    return data.debitCmd(map, 300);
  }

  private Flux<StateId> debitedIds(int n) {
    return data.createIds(0, n / 2).map(StateId::of);
  }

  private Flux<StateId> creditedIds(int n) {
    return data.createIds(n / 2, n / 2).map(StateId::of);
  }

  private static int expectedEventsCount(int numOfAccounts) {
    return (numOfAccounts / 2) * 5;
  }
}
