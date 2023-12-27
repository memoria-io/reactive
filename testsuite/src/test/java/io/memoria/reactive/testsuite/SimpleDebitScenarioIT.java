package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
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

import java.time.Duration;
import java.util.stream.Stream;

@TestMethodOrder(OrderAnnotation.class)
class SimpleDebitScenarioIT {
  private static final Logger log = LoggerFactory.getLogger(SimpleDebitScenarioIT.class.getName());

  // Infra
  private static final Data data = Data.ofUUID();
  private static final Configs configs = new Configs(System.currentTimeMillis() + "", 1, 1, 0);

  // Test case
  private static final int INITIAL_BALANCE = 500;
  private static final int DEBIT_AMOUNT = 300;
  private static final int numOfAccounts = 1000;
  private static final int count = 1000;

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("adapters")
  void simpleScenario(PartitionPipeline pipeline) {
    // Given
    int expectedCommandsCount = numOfAccounts * 3;
    int expectedEventsCount = numOfAccounts * 5;

    // When
    StepVerifier.create(commands().flatMap(pipeline.commandRepo::publish))
                .expectNextCount(expectedCommandsCount)
                .verifyComplete();
    // Then
    StepVerifier.create(pipeline.handle(pipeline.commandRepo.subscribe()))
                .expectNextCount(expectedEventsCount)
                .expectTimeout(Duration.ofMillis(5000))
                .verify();
    // And
//    StepVerifier.create(verify(pipeline, expectedCommandsCount)).expectNext(true).verifyComplete();
  }

  private Flux<AccountCommand> commands() {
    var debitedIds = data.createIds(0, numOfAccounts).map(StateId::of);
    var creditedIds = data.createIds(numOfAccounts, numOfAccounts).map(StateId::of);
    var createDebitedAcc = data.createAccountCmd(debitedIds, INITIAL_BALANCE);
    var createCreditedAcc = data.createAccountCmd(creditedIds, INITIAL_BALANCE);
    var debitTheAccounts = data.debitCmd(debitedIds.zipWith(creditedIds), DEBIT_AMOUNT);
    return createDebitedAcc.concatWith(createCreditedAcc).concatWith(debitTheAccounts);
  }

  private Mono<Boolean> verify(PartitionPipeline pipeline, int expectedEventsCount) {
    return Utils.reduce(pipeline.domain.evolver(),
                        pipeline.eventRepo.subscribe(configs.eventPartition).take(expectedEventsCount))
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
    return Stream.of(Arguments.of(Named.of("In memory", Infra.inMemoryPipeline(configs, data.domain()))),
                     Arguments.of(Named.of("Kafka", Infra.kafkaPipeline(configs, data.domain()))),
                     Arguments.of(Named.of("Nats", Infra.natsPipeline(configs, data.domain()))));
  }

  private static void printRates(String methodName, long start, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - start;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    var eventsPerSec = msgCount / (totalElapsed / 1000d);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec);
  }
}
