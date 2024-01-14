package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

class PartitionPipelineTest {
  private final Infra infra = new Infra();
  private final Data data = Data.ofSerial();
  private final CommandRoute commandRoute = new CommandRoute("commands");
  private final EventRoute eventRoute = new EventRoute("events");
  private static final int NUM_OF_ACCOUNTS = 10;
  private static final int EXPECTED_EVENTS_COUNT = (NUM_OF_ACCOUNTS / 2) * 5;
  private static final Duration timeout = Duration.ofMillis(100);

  @Test
  void happyPath() {
    var pipeline = infra.inMemoryPipeline(data.domain(), commandRoute, eventRoute);
    simpleDebitProcess(NUM_OF_ACCOUNTS).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.handle())
                .expectNextCount(EXPECTED_EVENTS_COUNT)
                .expectTimeout(Duration.ofMillis(100))
                .verify();
  }

  @Test
  void saga() {

  }

  @Test
  void evolving() {

  }

  @Test
  void atLeastOnceEvents() {
    // Given
    var pipeline = infra.inMemoryPipeline(data.domain(), commandRoute, eventRoute);
    simpleDebitProcess(NUM_OF_ACCOUNTS).concatMap(pipeline::publishCommand).subscribe();
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
    var restartedPipeline = infra.inMemoryPipeline(data.domain(), commandRoute, eventRoute);
    StepVerifier.create(restartedPipeline.handle())
                .expectNextCount(EXPECTED_EVENTS_COUNT)
                .expectTimeout(timeout)
                .verify();
  }

  @Test
  void duplicateCommand() {

  }

  @Test
  void duplicateSagaCommand() {

  }

  @Test
  void wrongState() {

  }

  @Test
  void invalidSequence() {

  }

  public Flux<AccountCommand> simpleDebitProcess(int accounts) {
    var debitedIds = data.createIds(0, accounts / 2).map(StateId::of);
    var creditedIds = data.createIds(accounts / 2, accounts / 2).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, 500);
    var createCreditedAcc = data.createAccountCmd(creditedIds, 500);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), 300);
    return createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);
  }
}