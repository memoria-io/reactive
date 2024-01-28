package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.vavr.collection.Map;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.stream.Stream;

@TestMethodOrder(OrderAnnotation.class)
class SinglePartitionIT {
  private static final Logger log = LoggerFactory.getLogger(SinglePartitionIT.class.getName());

  // Infra
  private static final Data data = Data.ofUUID();
  private static final Infra infra = new Infra("testGroup");

  // Test case
  private static final int INITIAL_BALANCE = 500;
  private static final int DEBIT_AMOUNT = 300;
  private static final int NUM_OF_DEBITORS = 1000;
  private static final int NUM_OF_CREDITORS = NUM_OF_DEBITORS;
  private static final int EXPECTED_COMMANDS_COUNT = NUM_OF_DEBITORS * 3;
  private static final int EXPECTED_EVENTS_COUNT = NUM_OF_DEBITORS * 5;

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("adapters")
  void simpleScenario(PartitionPipeline pipeline) {
    // Given
    StepVerifier.create(simpleDebitProcess().flatMap(pipeline::publishCommand))
                .expectNextCount(EXPECTED_COMMANDS_COUNT)
                .verifyComplete();
    // When
    StepVerifier.create(pipeline.handle().take(EXPECTED_EVENTS_COUNT))
                .expectNextCount(EXPECTED_EVENTS_COUNT)
                .verifyComplete();
    // Then
    StepVerifier.create(verify(pipeline)).expectNext(true).verifyComplete();
  }

  private Mono<Boolean> verify(PartitionPipeline pipeline) {
    return Utils.reduce(pipeline.getDomain().evolver(), pipeline.subscribeToEvents().take(EXPECTED_EVENTS_COUNT))
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

  public Flux<AccountCommand> simpleDebitProcess() {
    var debitedIds = data.createIds(0, NUM_OF_DEBITORS).map(StateId::of);
    var creditedIds = data.createIds(NUM_OF_CREDITORS, NUM_OF_CREDITORS).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = data.createAccountCmd(creditedIds, INITIAL_BALANCE);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), DEBIT_AMOUNT);
    return createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);
  }

  public static Stream<Arguments> adapters() {
    var commandRoute = new CommandRoute("commands" + System.currentTimeMillis(), 0, 1);
    var eventRoute = new EventRoute("events" + System.currentTimeMillis(), 0, 1);

    // In memory
    var inMemory = infra.inMemoryPipeline(data.domain(), commandRoute, eventRoute);

    // Kafka
    infra.createKafkaTopics(commandRoute.topic(), commandRoute.totalPartitions());
    infra.createKafkaTopics(eventRoute.topic(), eventRoute.totalPartitions());
    var kafka = infra.kafkaPipeline(data.domain(), commandRoute, eventRoute);

    // Nats
    infra.createNatsTopics(commandRoute.topic());
    infra.createNatsTopics(eventRoute.topic());
    var nats = infra.natsPipeline(data.domain(), commandRoute, eventRoute);
    return Stream.of(Arguments.of(Named.of("In memory", inMemory)),
                     Arguments.of(Named.of("Kafka", kafka)),
                     Arguments.of(Named.of("Nats", nats)));
  }
}
