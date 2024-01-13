package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class PartitionPipelineTest {
  private final Infra infra = new Infra();
  private final Data data = Data.ofSerial();
  private final CommandRoute commandRoute = new CommandRoute("commands");
  private final EventRoute eventRoute = new EventRoute("events");
  private static final int NUM_OF_ACCOUNTS = 10;
  private static final int EXPECTED_EVENTS_COUNT = (NUM_OF_ACCOUNTS / 2) * 5;

  @Test
  void happyPath() {
    var pipeline = infra.inMemoryPipeline(data.domain(), commandRoute, eventRoute);
    simpleDebitProcess(NUM_OF_ACCOUNTS).concatMap(pipeline::publishCommand).subscribe();
    StepVerifier.create(pipeline.handle().take(EXPECTED_EVENTS_COUNT))
                .expectNextCount(EXPECTED_EVENTS_COUNT)
                .verifyComplete();
  }

  @Test
  void duplicateEvent() {

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
    var creditedIds = data.createIds(accounts / 2, accounts).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, 500);
    var createCreditedAcc = data.createAccountCmd(creditedIds, 500);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), 300);
    return createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);
  }
}
