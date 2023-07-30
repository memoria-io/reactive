package io.memoria.reactive.nats.eventsourcing.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.eventsourcing.pipeline.partition.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingInfra;
import io.memoria.reactive.testsuite.eventsourcing.banking.scenario.PerformanceScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.scenario.SimpleDebitScenario;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.nats.eventsourcing.NatsCommandStream;
import io.memoria.reactive.nats.eventsourcing.NatsEventStream;
import io.nats.client.JetStreamApiException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import static io.memoria.reactive.nats.TestUtils.natsConfig;

class EventSourcingScenarioTest {
  private static final TextTransformer transformer = new SerializableTransformer();
  private static final String commandsTopicPrefix = "commands";
  private static final String eventsTopicPrefix = "events";
  private static final Duration timeout = Duration.ofMillis(300);
  private static final Random r = new Random();
  private static final BankingData BANKING_DATA = BankingData.ofUUID();

  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 5, 10, 20})
  void simpleDebitScenario(int numOfAccounts) throws JetStreamApiException, IOException, InterruptedException {
    // Given
    int randomPostFix = r.nextInt(1000);
    var commandRoute = new CommandRoute(toCommandTopic(numOfAccounts, randomPostFix), 0);
    var eventRoute = new EventRoute(toEventTopic(numOfAccounts, randomPostFix), 0);
    var pipeline = createPipeline(commandRoute, eventRoute);

    // When
    createTopics(commandRoute, eventRoute);
    var scenario = new SimpleDebitScenario(BANKING_DATA, pipeline, numOfAccounts);

    // Then
    var now = System.currentTimeMillis();
    StepVerifier.create(scenario.handleCommands()).expectNextCount(numOfAccounts * 5L).expectTimeout(timeout).verify();
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
    int randomPostFix = r.nextInt(1000);
    var commandRoute = new CommandRoute(toCommandTopic(numOfAccounts, randomPostFix), 0);
    var eventRoute = new EventRoute(toEventTopic(numOfAccounts, randomPostFix), 0);
    var pipeline = createPipeline(commandRoute, eventRoute);

    // When
    createTopics(commandRoute, eventRoute);
    var scenario = new PerformanceScenario(BANKING_DATA, pipeline, numOfAccounts);

    // Then
    StepVerifier.create(scenario.handleCommands()).expectNextCount(numOfAccounts * 5L).expectTimeout(timeout).verify();
  }

  @Disabled("Manual check")
  @Test
  void manualCheck() throws JetStreamApiException, IOException, InterruptedException {
    // Given
    int numOfAccounts = 20;
    int randomPostFix = 232;
    var commandRoute = new CommandRoute(toCommandTopic(numOfAccounts, randomPostFix), 0);
    var eventRoute = new EventRoute(toEventTopic(numOfAccounts, randomPostFix), 0);
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

  private String toCommandTopic(int numOfAccounts, int random) {
    return "%d-%s-%d".formatted(numOfAccounts, commandsTopicPrefix, random);
  }

  private String toEventTopic(int numOfAccounts, int random) {
    return "%d-%s-%d".formatted(numOfAccounts, eventsTopicPrefix, random);
  }

  private PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline(CommandRoute commandRoute,
                                                                                  EventRoute eventRoute) {
    try {
      // Streams
      var commandStream = new NatsCommandStream<>(natsConfig,
                                                  AccountCommand.class,
                                                  transformer,
                                                  Schedulers.boundedElastic());
      var eventStream = new NatsEventStream<>(natsConfig, AccountEvent.class, transformer, Schedulers.boundedElastic());

      // Pipeline
      return BankingInfra.createPipeline(BANKING_DATA.idSupplier,
                                         BANKING_DATA.timeSupplier,
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
