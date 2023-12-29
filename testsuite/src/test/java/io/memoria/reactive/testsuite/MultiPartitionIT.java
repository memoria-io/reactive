package io.memoria.reactive.testsuite;

import io.memoria.atom.testsuite.eventsourcing.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandRoute;
import io.memoria.reactive.eventsourcing.stream.EventRoute;
import io.vavr.Tuple;
import io.vavr.Tuple2;
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

import java.time.Duration;
import java.util.stream.Stream;

@TestMethodOrder(OrderAnnotation.class)
class MultiPartitionIT {
  private static final Logger log = LoggerFactory.getLogger(MultiPartitionIT.class.getName());

  // Infra
  private static final Data data = Data.ofUUID();
  private static final Infra infra = configs();

  // Test case
  private static final int INITIAL_BALANCE = 500;
  private static final int DEBIT_AMOUNT = 300;
  private static final int numOfAccounts = 100;

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("adapters")
  void simpleScenario(List<PartitionPipeline> pipelines) {
    // Given
    int expectedCommandsCount = numOfAccounts * 3;
    var pubCommands = data.simpleDebitProcess(numOfAccounts, INITIAL_BALANCE, DEBIT_AMOUNT)
                          .flatMap(pipelines.head().commandRepo::pub);
    StepVerifier.create(pubCommands).expectNextCount(expectedCommandsCount).verifyComplete();

    // When
    //    pipelines.map(PartitionPipeline::handle).map(Flux::subscribe);
    //    // Then
    //    pipelines.forEach(p -> StepVerifier.create(verify(p)).expectNext(true).verifyComplete());
  }

  private Mono<Boolean> verify(PartitionPipeline pipeline) {
    return Utils.reduce(pipeline.domain.evolver(), pipeline.eventRepo.sub().take(Duration.ofMillis(1000)))
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
    return Stream.of(Arguments.of(Named.of("Kafka", getRoutes(1).map(MultiPartitionIT::kafkaPipeline))),
                     Arguments.of(Named.of("Nats", getRoutes(2).map(MultiPartitionIT::natsPipeline))));
  }

  private static List<Tuple2<CommandRoute, EventRoute>> getRoutes(int x) {
    var cTopic = "commands" + System.currentTimeMillis();
    var eTopic = "events" + System.currentTimeMillis();
    return List.range(0, x).map(i -> Tuple.of(new CommandRoute(cTopic, i, x), new EventRoute(eTopic, i, x)));
  }

  private static PartitionPipeline kafkaPipeline(Tuple2<CommandRoute, EventRoute> routes) {
    return infra.kafkaPipeline(data.domain(), routes._1, routes._2);
  }

  private static PartitionPipeline natsPipeline(Tuple2<CommandRoute, EventRoute> routes) {
    return infra.natsPipeline(data.domain(), routes._1, routes._2);
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
