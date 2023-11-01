package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.atom.testsuite.eventsourcing.banking.state.OpenAccount;
import io.memoria.reactive.core.stream.MsgStream;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.JetStreamApiException;
import io.vavr.collection.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.stream.Stream;

import static io.memoria.reactive.testsuite.Infra.NATS_CONFIG;
import static io.memoria.reactive.testsuite.Infra.StreamType.KAFKA;
import static io.memoria.reactive.testsuite.Infra.StreamType.MEMORY;
import static io.memoria.reactive.testsuite.Infra.StreamType.NATS;
import static io.memoria.reactive.testsuite.Infra.pipeline;
import static io.memoria.reactive.testsuite.Infra.randomTopicName;

@TestMethodOrder(OrderAnnotation.class)
class SimpleDebitScenarioIT {
  private static final Data data = Data.ofUUID();
  private static final CommandRoute commandRoute = new CommandRoute(randomTopicName("commands"), 0);
  private static final EventRoute eventRoute = new EventRoute(randomTopicName("events"), 0);
  private static final PartitionPipeline<Account, AccountCommand, AccountEvent> inMemoryPipeline = createPipeline(Infra.inMemoryStream);
  private static final PartitionPipeline<Account, AccountCommand, AccountEvent> kafkaPipeline = createPipeline(Infra.kafkaStream);
  private static final PartitionPipeline<Account, AccountCommand, AccountEvent> natsPipeline = createPipeline(Infra.natsStream);

  private static final int INITIAL_BALANCE = 500;
  private static final int DEBIT_AMOUNT = 300;
  private static final int numOfAccounts = 1000;

  @BeforeAll
  static void beforeAll() throws JetStreamApiException, IOException, InterruptedException {
    NatsUtils.createOrUpdateTopic(NATS_CONFIG, commandRoute.name(), commandRoute.totalPartitions());
    NatsUtils.createOrUpdateTopic(NATS_CONFIG, eventRoute.topicName(), eventRoute.totalPartitions());
  }

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("adapters")
  void simpleScenario(String name, PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline) {
    // Given
    int expectedCommandsCount = numOfAccounts * 3;
    int expectedEventsCount = numOfAccounts * 5;

    // When
    commands().flatMap(pipeline::publish).subscribe();

    // Then
    var start = System.currentTimeMillis();
    StepVerifier.create(pipeline.handle()).expectNextCount(expectedEventsCount).expectTimeout(Infra.TIMEOUT).verify();
    Infra.printRates(SimpleDebitScenarioIT.class.getName(), start, expectedEventsCount);
    // And
    StepVerifier.create(verify(pipeline, expectedCommandsCount)).expectNext(true).verifyComplete();
  }

  private Flux<AccountCommand> commands() {
    var debitedIds = data.createIds(0, numOfAccounts).map(StateId::of);
    var creditedIds = data.createIds(numOfAccounts, numOfAccounts).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = data.createAccountCmd(creditedIds, INITIAL_BALANCE);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), DEBIT_AMOUNT);
    return createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);
  }

  private Mono<Boolean> verify(PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline,
                               int expectedEventsCount) {
    return Utils.reduce(pipeline.domain.evolver(), pipeline.subToEvents().take(expectedEventsCount))
                .map(Map::values)
                .flatMapMany(Flux::fromIterable)
                .map(OpenAccount.class::cast)
                .map(this::verify)
                .reduce((a, b) -> a && b);
  }

  private boolean verify(OpenAccount acc) {
    if (acc.debitCount() > 0) {
      return acc.balance() == INITIAL_BALANCE - DEBIT_AMOUNT;
    } else if (acc.creditCount() > 0) {
      return acc.balance() == INITIAL_BALANCE + DEBIT_AMOUNT;
    } else {
      return acc.balance() == INITIAL_BALANCE;
    }
  }

  private static Stream<Arguments> adapters() {

    return Stream.of(Arguments.of(MEMORY.name(), inMemoryPipeline),
                     Arguments.of(KAFKA.name(), kafkaPipeline),
                     Arguments.of(NATS.name(), natsPipeline));
  }

  private static PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline(MsgStream msgStream) {
    return pipeline(data.idSupplier, data.timeSupplier, commandRoute, eventRoute, msgStream, true);
  }
}
