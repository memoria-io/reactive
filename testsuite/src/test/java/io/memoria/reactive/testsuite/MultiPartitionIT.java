package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
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
import java.util.concurrent.CountDownLatch;
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
  private static final int NUM_OF_DEBITORS = 10;
  private static final int NUM_OF_CREDITORS = NUM_OF_DEBITORS;
  private static final int EXPECTED_COMMANDS_COUNT = NUM_OF_DEBITORS * 3;
  private static final int EXPECTED_EVENTS_COUNT = NUM_OF_DEBITORS * 5;

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("adapters")
  void simpleScenario(List<PartitionPipeline> pipelines) {
    // Given
    StepVerifier.create(simpleDebitProcess().flatMap(pipelines.head().commandRepo::pub))
                .expectNextCount(EXPECTED_COMMANDS_COUNT)
                .verifyComplete();
    // When
    pipelines.map(PartitionPipeline::handle).map(Flux::subscribe);
    // Then
    var latch = new CountDownLatch(EXPECTED_EVENTS_COUNT);
    pipelines.forEach(p -> StepVerifier.create(verify(p, latch)).expectNext(true).verifyComplete());
  }

  private Mono<Boolean> verify(PartitionPipeline pipeline, CountDownLatch latch) {
    return Utils.reduce(pipeline.domain.evolver(), pipeline.eventRepo.sub().take(Duration.ofMillis(3000)))
                .map(Map::values)
                .flatMapMany(Flux::fromIterable)
                .map(OpenAccount.class::cast)
                .map(this::verify)
                .doOnNext(_ -> latch.countDown())
                .doOnNext(_ -> System.out.println(latch.getCount()))
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
    return Stream.of(
            //            Arguments.of(Named.of("Kafka", getRoutes(2).map(MultiPartitionIT::kafkaPipeline))),
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

  public Flux<AccountCommand> simpleDebitProcess() {
    var debitedIds = data.createIds(0, NUM_OF_DEBITORS).map(StateId::of);
    var creditedIds = data.createIds(NUM_OF_CREDITORS, NUM_OF_CREDITORS).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = data.createAccountCmd(creditedIds, INITIAL_BALANCE);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), DEBIT_AMOUNT);
    return createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);
  }

  private static Infra configs() {
    return new Infra("testGroup");
  }
}
