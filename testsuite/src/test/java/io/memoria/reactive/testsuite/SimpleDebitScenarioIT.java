package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.testsuite.eventsourcing.AccountDecider;
import io.memoria.atom.testsuite.eventsourcing.AccountEvolver;
import io.memoria.atom.testsuite.eventsourcing.AccountSaga;
import io.memoria.atom.testsuite.eventsourcing.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.state.OpenAccount;
import io.memoria.reactive.eventsourcing.Utils;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandRepo;
import io.memoria.reactive.eventsourcing.stream.EventRepo;
import io.memoria.reactive.kafka.KafkaCommandRepo;
import io.memoria.reactive.kafka.KafkaEventRepo;
import io.memoria.reactive.nats.NatsCommandRepo;
import io.memoria.reactive.nats.NatsEventRepo;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
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

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Stream;

import static io.memoria.reactive.testsuite.Infra.NATS_URL;
import static io.memoria.reactive.testsuite.Infra.StreamType.KAFKA;
import static io.memoria.reactive.testsuite.Infra.StreamType.MEMORY;
import static io.memoria.reactive.testsuite.Infra.StreamType.NATS;
import static io.memoria.reactive.testsuite.Infra.kafkaConsumerConfigs;
import static io.memoria.reactive.testsuite.Infra.kafkaProducerConfigs;

@TestMethodOrder(OrderAnnotation.class)
class SimpleDebitScenarioIT {
  private static final Logger log = LoggerFactory.getLogger(SimpleDebitScenarioIT.class.getName());

  // Infra
  private static final Data data = Data.ofUUID();
  private static final String commandsTopic = "commands_" + System.currentTimeMillis();
  private static final String eventsTopic = "events_" + System.currentTimeMillis();
  private static final int totalCommandPartitions = 1;
  private static final int totalEventPartitions = 1;
  private static final int eventPartition = 0;
  private static final SerializableTransformer transformer = new SerializableTransformer();

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
    commands().flatMap(pipeline.commandRepo::publish).subscribe();

    // Then
    var start = System.currentTimeMillis();

    StepVerifier.create(pipeline.handle(pipeline.commandRepo.subscribe()))
                .expectNextCount(expectedEventsCount)
                .expectTimeout(Infra.TIMEOUT)
                .verify();
    printRates(SimpleDebitScenarioIT.class.getName(), start, expectedEventsCount);
    // And
    StepVerifier.create(verify(pipeline, expectedCommandsCount)).expectNext(true).verifyComplete();
  }

  public static PartitionPipeline kafkaPipeline() {

    var commandRepo = new KafkaCommandRepo(kafkaProducerConfigs(),
                                           kafkaConsumerConfigs(),
                                           commandsTopic,
                                           totalCommandPartitions,
                                           transformer);
    var eventRepo = new KafkaEventRepo(kafkaProducerConfigs(),
                                       kafkaConsumerConfigs(),
                                       eventsTopic,
                                       totalEventPartitions,
                                       Duration.ofMillis(1000),
                                       transformer);
    return new PartitionPipeline(domain(), commandRepo, eventRepo, eventPartition);
  }

  private static PartitionPipeline natsPipeline() {
    try {
      var nc = NatsUtils.createConnection(NATS_URL);
      JetStreamManagement jsManagement = nc.jetStreamManagement();
      var commandRepo = new NatsCommandRepo(nc, commandsTopic, totalCommandPartitions, transformer);
      var eventRepo = new NatsEventRepo(nc, eventsTopic, totalEventPartitions, transformer);
      NatsUtils.createOrUpdateStream(jsManagement, commandsTopic, 1);
      NatsUtils.createOrUpdateStream(jsManagement, eventsTopic, 1);
      return new PartitionPipeline(domain(), commandRepo, eventRepo, eventPartition);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  private static PartitionPipeline inMemoryPipeline() {
    var commandRepo = CommandRepo.inMemory();
    var eventRepo = EventRepo.inMemory(0);
    return new PartitionPipeline(domain(), commandRepo, eventRepo, eventPartition);
  }

  private static Domain domain() {
    return new Domain(new AccountDecider(data.idSupplier, data.timeSupplier),
                      new AccountEvolver(),
                      new AccountSaga(data.idSupplier, data.timeSupplier));
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
                        pipeline.eventRepo.subscribe(eventPartition).take(expectedEventsCount))
                .map(Map::values)
                .flatMapMany(Flux::fromIterable)
                .map(OpenAccount.class::cast)
                .map(this::verify)
                .reduce((a, b) -> a && b);
  }

  private static double eventsPerSec(long msgCount, long totalElapsedMillis) {
    return msgCount / (totalElapsedMillis / 1000d);
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

    return Stream.of(Arguments.of(Named.of(MEMORY.name(), inMemoryPipeline())),
                     Arguments.of(Named.of(KAFKA.name(), kafkaPipeline())),
                     Arguments.of(Named.of(NATS.name(), natsPipeline())));
  }

  private static void printRates(String methodName, long start, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - start;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec(msgCount, totalElapsed));
  }
}
