package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.CommandMeta;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.EventMeta;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.eventsourcing.exceptions.InvalidEvolution;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.command.Credit;
import io.memoria.atom.testsuite.eventsourcing.event.AccountCreated;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

class PartitionPipelineTest {
  private static final int NUM_OF_ACCOUNTS = 10;
  private static final int NUM_OF_EXPECTED_EVENTS = (NUM_OF_ACCOUNTS / 2) * 5;
  private static final Duration timeout = Duration.ofMillis(100);

  private final Infra infra = new Infra();
  private final Data data = Data.ofSerial();
  private final Flux<StateId> debitedIds = data.createIds(0, NUM_OF_ACCOUNTS / 2).map(StateId::of);
  private final Flux<StateId> creditedIds = data.createIds(NUM_OF_ACCOUNTS / 2, NUM_OF_ACCOUNTS / 2).map(StateId::of);

  @Test
  void happyPath() {
    // Given
    var pipeline = createSimplePipeline();

    // When
    createAccounts().concatWith(debitAccounts()).concatMap(pipeline::publishCommand).subscribe();

    // Then
    StepVerifier.create(pipeline.handle())
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
    createAccounts().concatMap(pipeline::publishCommand).subscribe();

    // Then
    StepVerifier.create(pipeline.handle())
                .expectNextCount(NUM_OF_ACCOUNTS)
                .expectTimeout(Duration.ofMillis(100))
                .verify();

    // When old pipeline died
    pipeline = createSimplePipeline();
    debitAccounts().concatMap(pipeline::publishCommand).subscribe();

    // Then new pipeline should pick up last state
    StepVerifier.create(pipeline.handle())
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
    createAccounts().concatWith(debitAccounts()).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.handle()).expectNextCount(NUM_OF_EXPECTED_EVENTS).expectTimeout(timeout).verify();

    // When
    var lastEvent = pipeline.subscribeToEvents().take(NUM_OF_EXPECTED_EVENTS).last().block();
    pipeline.publishEvent(lastEvent).block();
    pipeline.publishEvent(lastEvent).block();

    // Then while event is duplicated
    StepVerifier.create(pipeline.subscribeToEvents().doOnNext(System.out::println))
                .expectNextCount(NUM_OF_EXPECTED_EVENTS + 2)
                .expectTimeout(timeout)
                .verify();

    // Event is still ignored
    var restartedPipeline = createSimplePipeline();
    StepVerifier.create(restartedPipeline.handle())
                .expectNextCount(NUM_OF_EXPECTED_EVENTS)
                .expectTimeout(timeout)
                .verify();
  }

  @Test
  @DisplayName("Command should be dropped when it's already handled")
  void isHandledCommand() {
    // Given
    var pipeline = createSimplePipeline();

    // When
    var createAccounts = requireNonNull(createAccounts().collectList().block());
    Flux.fromIterable(createAccounts).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.handle()).expectNextCount(NUM_OF_ACCOUNTS).expectTimeout(timeout).verify();

    // Then
    Assertions.assertThat(pipeline.isDuplicateCommand(createAccounts.getFirst())).isTrue();
  }

  @Test
  void duplicateSagaCommand() {
    // Given
    var pipeline = createSimplePipeline();

    // When
    createAccounts().concatWith(debitAccounts()).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.handle()).expectNextCount(NUM_OF_EXPECTED_EVENTS).expectTimeout(timeout).verify();

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
    pipeline.publishCommand(debitAccounts().blockFirst()).subscribe();

    // Then
    StepVerifier.create(pipeline.handle()).expectError(InvalidEvolution.class).verify();
  }

  @Test
  void invalidEventSequence() {

  }

  private PartitionPipeline createSimplePipeline() {
    return infra.inMemoryPipeline(data.domain(), new CommandRoute("commands"), new EventRoute("events"));
  }

  private Flux<AccountCommand> createAccounts() {
    var createDebitedAcc = data.createAccountCmd(debitedIds, 500);
    var createCreditedAcc = data.createAccountCmd(creditedIds, 500);
    return createDebitedAcc.concatWith(createCreditedAcc);
  }

  private Flux<AccountCommand> debitAccounts() {
    return data.debitCmd(debitedIds.zipWith(creditedIds), 300);
  }
}
