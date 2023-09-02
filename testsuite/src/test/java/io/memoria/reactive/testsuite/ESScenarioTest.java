package io.memoria.reactive.testsuite;

import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.core.stream.MsgStream;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.JetStreamApiException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.stream.Stream;

import static io.memoria.reactive.testsuite.Infra.NATS_CONFIG;
import static io.memoria.reactive.testsuite.Infra.StreamType.KAFKA;
import static io.memoria.reactive.testsuite.Infra.StreamType.MEMORY;
import static io.memoria.reactive.testsuite.Infra.StreamType.NATS;
import static io.memoria.reactive.testsuite.Infra.msgStream;
import static io.memoria.reactive.testsuite.Infra.pipeline;
import static io.memoria.reactive.testsuite.Infra.topicName;

@TestMethodOrder(OrderAnnotation.class)
class ESScenarioTest {
  private static final Data data = Data.ofUUID();
  private static final CommandRoute commandRoute = new CommandRoute(topicName("commands"), 0);
  private static final EventRoute eventRoute = new EventRoute(topicName("events"), 0);
  private static final MsgStream inMemoryStream = msgStream(MEMORY);
  private static final MsgStream kafkaStream = msgStream(KAFKA);
  private static final MsgStream natsStream = msgStream(NATS);

  @BeforeAll
  static void beforeAll() throws JetStreamApiException, IOException, InterruptedException {
    NatsUtils.createOrUpdateTopic(NATS_CONFIG, commandRoute.name(), commandRoute.totalPartitions());
    NatsUtils.createOrUpdateTopic(NATS_CONFIG, eventRoute.topicName(), eventRoute.totalPartitions());
  }

  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("emptyStreams")
  @Order(0)
  void simpleScenario(String name,
                      Data data,
                      PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline,
                      int numOfAccounts) {
    // When
    var scenario = new SimpleDebitScenario(data, pipeline, numOfAccounts);
    StepVerifier.create(scenario.publishCommands()).expectNextCount(numOfAccounts * 3L).verifyComplete();

    // Then
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(scenario.expectedEventsCount())
                .expectTimeout(Infra.TIMEOUT)
                .verify();
  }

  @ParameterizedTest(name = "Restart simulation with enabled saga on startup, Using {0} adapter")
  @MethodSource("nonEmptyStream")
  @Order(1)
  void restartSimulation(String name,
                         Data data,
                         PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline,
                         int numOfAccounts) {
    // When
    var scenario = new SimpleDebitScenario(data, pipeline, numOfAccounts);

    // Then
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(scenario.expectedEventsCount())
                .expectTimeout(Infra.TIMEOUT)
                .verify();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} adapter")
  @MethodSource("emptyStreams")
  void performance(String name, Data data, PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline) {
    // Given
    int numOfAccounts = 1000_000;

    // When
    var scenario = new PerformanceScenario(data, pipeline, numOfAccounts);
    StepVerifier.create(scenario.publishCommands()).expectNextCount(numOfAccounts * 3L).verifyComplete();
    // Then
    StepVerifier.create(scenario.handleCommands())
                .expectNextCount(numOfAccounts * 5L)
                .expectTimeout(Infra.TIMEOUT)
                .verify();
  }

  private static Stream<Arguments> emptyStreams() {
    // StartupSaga = true, has no effect since the pipeline is initial and empty (no events)
    var inMemoryPipeline = pipeline(data.idSupplier, data.timeSupplier, commandRoute, eventRoute, inMemoryStream, true);
    var kafkaPipeline = pipeline(data.idSupplier, data.timeSupplier, commandRoute, eventRoute, kafkaStream, true);
    var natsPipeline = pipeline(data.idSupplier, data.timeSupplier, commandRoute, eventRoute, natsStream, true);
    return Stream.of(Arguments.of(MEMORY.name(), data, inMemoryPipeline, 10),
                     Arguments.of(KAFKA.name(), data, kafkaPipeline, 10),
                     Arguments.of(NATS.name(), data, natsPipeline, 10));
  }

  private static Stream<Arguments> nonEmptyStream() {
    var inMemoryPipeline = pipeline(data.idSupplier, data.timeSupplier, commandRoute, eventRoute, inMemoryStream, true);
    var kafkaPipeline = pipeline(data.idSupplier, data.timeSupplier, commandRoute, eventRoute, kafkaStream, true);
    var natsPipeline = pipeline(data.idSupplier, data.timeSupplier, commandRoute, eventRoute, natsStream, true);

    return Stream.of(Arguments.of(MEMORY.name(), data, inMemoryPipeline, 10),
                     Arguments.of(KAFKA.name(), data, kafkaPipeline, 10),
                     Arguments.of(NATS.name(), data, natsPipeline, 10));
  }
}
