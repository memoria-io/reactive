package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.vavr.collection.List;
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

import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

@TestMethodOrder(OrderAnnotation.class)
class MultiPartitionIT {
  private static final Logger log = LoggerFactory.getLogger(MultiPartitionIT.class.getName());
  // Infra
  private static final Data data = Data.ofUUID();
  private static final Infra infra = new Infra("testGroup");
  private static final int numOfPipelines = 10;

  // Test case
  private static final int INITIAL_BALANCE = 500;
  private static final int DEBIT_AMOUNT = 300;
  private static final int NUM_OF_DEBITORS = 100;
  private static final int NUM_OF_CREDITORS = NUM_OF_DEBITORS;
  private static final int TOTAL_EVENTS_COUNT = NUM_OF_DEBITORS * 5;
  private final Flux<StateId> debitedIds = data.createIds(0, NUM_OF_DEBITORS).map(StateId::of);
  private final Flux<StateId> creditedIds = data.createIds(NUM_OF_CREDITORS, NUM_OF_CREDITORS).map(StateId::of);

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("adapters")
  void multiPartitionScenario(List<PartitionPipeline> pipelines) throws InterruptedException {
    // Given
    StepVerifier.create(createAccounts().flatMap(pipelines.head()::publishCommand))
                .expectNextCount(NUM_OF_CREDITORS + NUM_OF_DEBITORS)
                .verifyComplete();
    StepVerifier.create(debitAccounts().flatMap(pipelines.head()::publishCommand))
                .expectNextCount(NUM_OF_DEBITORS)
                .verifyComplete();

    // When
    var latch = new CountDownLatch(TOTAL_EVENTS_COUNT);
    pipelines.forEach(p -> subscribe(p, latch));
    latch.await();

    // Then
    var pipelineEventsCount = TOTAL_EVENTS_COUNT / pipelines.size();
    pipelines.forEach(p -> StepVerifier.create(verify(p, pipelineEventsCount)).expectNext(true).verifyComplete());
  }

  private Flux<AccountCommand> createAccounts() {
    var createDebitedAcc = data.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = data.createAccountCmd(creditedIds, INITIAL_BALANCE);
    return createDebitedAcc.concatWith(createCreditedAcc);
  }

  private Flux<AccountCommand> debitAccounts() {
    return data.debitCmd(debitedIds.zipWith(creditedIds), DEBIT_AMOUNT);
  }

  private static void subscribe(PartitionPipeline p, CountDownLatch latch) {
    p.handle().doOnNext(_ -> latch.countDown()).subscribe();
  }

  private Mono<Boolean> verify(PartitionPipeline pipeline, int expectedEventsCount) {
    return Utils.reduce(pipeline.getDomain().evolver(), pipeline.subscribeToEvents().take(expectedEventsCount))
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
    var routes = infra.getRoutes(numOfPipelines);
    var commandRoute = routes.head()._1;
    var eventRoute = routes.head()._2;

    // In memory
    var inMemory = routes.map(tup -> infra.inMemoryPipeline(data.domain(), tup._1, tup._2));
    var inMemoryArgs = Arguments.of(Named.of("In memory", inMemory));
    // Nats
    infra.createNatsTopics(commandRoute.topic());
    infra.createNatsTopics(eventRoute.topic());
    var nats = routes.map(tup -> infra.natsPipeline(data.domain(), tup._1, tup._2));
    var natsArgs = Arguments.of(Named.of("Nats", nats));
    // Kafka
    infra.createKafkaTopics(commandRoute.topic(), commandRoute.totalPartitions());
    infra.createKafkaTopics(eventRoute.topic(), eventRoute.totalPartitions());
    var kafka = routes.map(tup -> infra.kafkaPipeline(data.domain(), tup._1, tup._2));
    var kafkaArgs = Arguments.of(Named.of("Kafka", kafka));

    return Stream.of(inMemoryArgs, kafkaArgs, natsArgs);
  }
}
