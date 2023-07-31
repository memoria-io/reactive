package io.memoria.reactive.nats.eventsourcing.pipeline.partition;

import io.memoria.reactive.eventsourcing.pipeline.partition.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.nats.eventsourcing.stream.NatsCommandStream;
import io.memoria.reactive.nats.eventsourcing.stream.NatsEventStream;
import io.memoria.reactive.testsuite.TestsuiteUtils;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingInfra;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition.PerformanceScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.pipeline.partition.SimpleDebitScenario;
import io.nats.client.JetStreamApiException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.io.IOException;

import static io.memoria.reactive.nats.TestUtils.natsConfig;
import static io.memoria.reactive.testsuite.TestsuiteUtils.*;

class ESScenarioTest {
  private static final BankingData data = BankingData.ofUUID();

  @Test
  void simpleDebitScenario() throws JetStreamApiException, IOException, InterruptedException {
    // Given
    var commandRoute = new CommandRoute(TestsuiteUtils.topicName("commands"), 0);
    var eventRoute = new EventRoute(TestsuiteUtils.topicName("events"), 0);
    var pipeline = createPipeline(commandRoute, eventRoute);

    // When
    createTopics(commandRoute, eventRoute);
    var scenario = new SimpleDebitScenario(data, pipeline, MSG_COUNT);
    StepVerifier.create(scenario.publishCommands()).expectNextCount(MSG_COUNT * 3).verifyComplete();

    // Then
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.handleCommands()).expectNextCount(MSG_COUNT * 5L).expectTimeout(TIMEOUT).verify();
    System.out.println(System.currentTimeMillis() - now);
    //    StepVerifier.create(scenario.verify(scenario.handle()))
    //                .expectNextCount(numOfAccounts * 5L)
    //                .expectTimeout(timeout)
    //                .verify();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) throws JetStreamApiException, IOException, InterruptedException {
    // Given
    var commandRoute = new CommandRoute(TestsuiteUtils.topicName("commands"), 0);
    var eventRoute = new EventRoute(TestsuiteUtils.topicName("events"), 0);
    var pipeline = createPipeline(commandRoute, eventRoute);

    // When
    createTopics(commandRoute, eventRoute);
    var scenario = new PerformanceScenario(data, pipeline, numOfAccounts);

    // Then
    StepVerifier.create(scenario.handleCommands()).expectNextCount(numOfAccounts * 5L).expectTimeout(TIMEOUT).verify();
  }

  @Disabled("Manual check")
  @Test
  void manualCheck() throws JetStreamApiException, IOException, InterruptedException {
    // Given
    var commandRoute = new CommandRoute(TestsuiteUtils.topicName("commands"), 0);
    var eventRoute = new EventRoute(TestsuiteUtils.topicName("events"), 0);
    var pipeline = createPipeline(commandRoute, eventRoute);

    // When
    createTopics(commandRoute, eventRoute);
    var commands = pipeline.subToCommands().doOnNext(System.out::println);
    var events = pipeline.subToEvents().doOnNext(System.out::println);

    // Then
    //    StepVerifier.create(commands).expectNextCount(numOfAccounts * 5L).expectTimeout(Duration.ofMillis(500)).verify();
    StepVerifier.create(events).expectNextCount(10).verifyComplete();
  }

  private static void createTopics(CommandRoute commandRoute, EventRoute eventRoute)
          throws IOException, InterruptedException, JetStreamApiException {
    System.out.printf("Creating %s %n", commandRoute);
    System.out.printf("Creating %s %n", eventRoute);
    NatsUtils.createOrUpdateTopic(natsConfig, commandRoute.topicName(), commandRoute.totalPartitions());
    NatsUtils.createOrUpdateTopic(natsConfig, eventRoute.topicName(), eventRoute.totalPartitions());
  }

  private PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline(CommandRoute commandRoute,
                                                                                  EventRoute eventRoute) {
    try {
      // Streams
      var commandStream = new NatsCommandStream<>(natsConfig,
                                                  AccountCommand.class,
                                                  SERIALIZABLE_TRANSFORMER,
                                                  SCHEDULER);
      var eventStream = new NatsEventStream<>(natsConfig, AccountEvent.class, SERIALIZABLE_TRANSFORMER, SCHEDULER);

      // Pipeline
      return BankingInfra.createPipeline(data.idSupplier,
                                         data.timeSupplier,
                                         commandStream,
                                         commandRoute,
                                         eventStream,
                                         eventRoute);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
