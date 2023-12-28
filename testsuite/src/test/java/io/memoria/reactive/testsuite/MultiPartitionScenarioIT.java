package io.memoria.reactive.testsuite;

import io.memoria.atom.testsuite.eventsourcing.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandRoute;
import io.memoria.reactive.eventsourcing.stream.EventRoute;
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
class MultiPartitionScenarioIT {
  private static final Logger log = LoggerFactory.getLogger(MultiPartitionScenarioIT.class.getName());

  // Infra
  private static final Data data = Data.ofUUID();
  private static final Infra infra = configs();

  // Test case
  private static final int INITIAL_BALANCE = 500;
  private static final int DEBIT_AMOUNT = 300;
  private static final int numOfAccounts = 100;

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("adapters")
  void simpleScenario(PartitionPipeline pipeline) {
    // Given
    int expectedCommandsCount = numOfAccounts * 3;
    int expectedEventsCount = numOfAccounts * 5;

    // When
    var commands = data.simpleDebitProcess(numOfAccounts, INITIAL_BALANCE, DEBIT_AMOUNT);
    StepVerifier.create(commands.flatMap(pipeline.commandRepo::publish))
                .expectNextCount(expectedCommandsCount)
                .verifyComplete();
    // Then
    StepVerifier.create(pipeline.handle(pipeline.commandRepo.subscribe()).take(expectedEventsCount))
                .expectNextCount(expectedEventsCount)
                .verifyComplete();
    // And
    StepVerifier.create(verify(pipeline, expectedCommandsCount)).expectNext(true).verifyComplete();
  }

  private Mono<Boolean> verify(PartitionPipeline pipeline, int expectedEventsCount) {
    return Utils.reduce(pipeline.domain.evolver(), pipeline.eventRepo.subscribe().take(expectedEventsCount))
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
    var eventRoute = new EventRoute("events" + System.currentTimeMillis(), 0, 1);
    var commandRoute = new CommandRoute("commands" + System.currentTimeMillis(), 1);
    var inMemory = infra.inMemoryPipeline(data.domain());
    var kafka = infra.kafkaPipeline(data.domain(), commandRoute, eventRoute);
    var nats = infra.natsPipeline(data.domain(), commandRoute, eventRoute);
    return Stream.of(Arguments.of(Named.of("In memory", inMemory)),
                     Arguments.of(Named.of("Kafka", kafka)),
                     Arguments.of(Named.of("Nats", nats)));
  }

  private static void printRates(String methodName, long start, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - start;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    var eventsPerSec = msgCount / (totalElapsed / 1000d);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec);
  }

  private static Infra configs() {
    return new Infra("testGroup");
  }
}
