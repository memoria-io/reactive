package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

class PartitionPipelineTest {
  private static final int NUM_OF_ACCOUNTS = 10;
  private static final int EXPECTED_EVENTS_COUNT = (NUM_OF_ACCOUNTS / 2) * 5;
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
                .expectNextCount(EXPECTED_EVENTS_COUNT)
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
                .expectNextCount(EXPECTED_EVENTS_COUNT)
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
  void evolve() {

  }

  @Test
  void saga() {

  }

  @Test
  void atLeastOnceEvents() {
    // Given
    var pipeline = createSimplePipeline();
    createAccounts().concatWith(debitAccounts()).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.handle()).expectNextCount(EXPECTED_EVENTS_COUNT).expectTimeout(timeout).verify();

    // When
    var lastEvent = pipeline.subscribeToEvents().take(EXPECTED_EVENTS_COUNT).last().block();
    pipeline.publishEvent(lastEvent).block();
    pipeline.publishEvent(lastEvent).block();

    // Then while event is duplicated
    StepVerifier.create(pipeline.subscribeToEvents().doOnNext(System.out::println))
                .expectNextCount(EXPECTED_EVENTS_COUNT + 2)
                .expectTimeout(timeout)
                .verify();

    // Event is still ignored
    var restartedPipeline = createSimplePipeline();
    StepVerifier.create(restartedPipeline.handle())
                .expectNextCount(EXPECTED_EVENTS_COUNT)
                .expectTimeout(timeout)
                .verify();
  }

  @Test
  @DisplayName("Command should be dropped when it's already handled")
  void isHandledCommand() {
    // Given
    var pipeline = createSimplePipeline();

    // When
    var createAccounts = createAccounts().toStream().toList();
    Flux.fromIterable(createAccounts).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.handle()).expectNextCount(NUM_OF_ACCOUNTS).expectTimeout(timeout).verify();

    // Then
    Assertions.assertThat(pipeline.isHandledCommand(createAccounts.getFirst())).isTrue();
  }

  // TODO
  @Test
  void duplicateSagaCommand() {
    // Given
    var pipeline = createSimplePipeline();

    // When
    createAccounts().concatWith(debitAccounts()).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.handle()).expectNextCount(NUM_OF_ACCOUNTS).expectTimeout(timeout).verify();
    var firstCommand = createAccounts().blockFirst();

    // Then
    //    assert firstCommand != null;
    //    Assertions.assertThat(pipeline.isHandledSagaCommand(firstCommand)).isTrue();
    assert false;
  }

  @Test
  void wrongState() {

  }

  @Test
  void invalidSequence() {

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
