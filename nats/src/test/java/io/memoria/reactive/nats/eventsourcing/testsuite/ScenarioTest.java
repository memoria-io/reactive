package io.memoria.reactive.nats.eventsourcing.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.eventsourcing.pipeline.partition.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.Account;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.Data;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.Infra;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.PerformanceScenario;
import io.memoria.reactive.eventsourcing.testsuite.banking.scenario.SimpleDebitScenario;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.eventsourcing.NatsCommandStream;
import io.memoria.reactive.nats.eventsourcing.NatsEventStream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import static io.memoria.reactive.nats.Utils.NATS_URL;
import static io.memoria.reactive.nats.Utils.createConfig;

class ScenarioTest {
  private static final TextTransformer transformer = new SerializableTransformer();
  private static final String commandsTopicPrefix = "commands";
  private static final String eventsTopicPrefix = "events";
  private final Random r = new Random();
  private final int randomPostFix = r.nextInt(1000);

  // Pipeline
  private final Data data = Data.ofSerial();
  //  private final Data data = Data.ofUUID();

  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {20})
  void simpleDebitScenario(int numOfAccounts) {
    // Given
    var commandRoute = new CommandRoute(toCommandTopic(numOfAccounts, randomPostFix), 0);
    var eventRoute = new EventRoute(toEventTopic(numOfAccounts, randomPostFix), 0);
    System.out.println(commandRoute + " : " + eventRoute);
    var pipeline = createPipeline(commandRoute, eventRoute);

    // When
    var scenario = new SimpleDebitScenario(data, pipeline, numOfAccounts);
    //    var pub = Flux.range(1000, 20).map(Id::of).concatMap(id -> pipeline.pubEvent(data.createAccountEvent(id, 100000)));

    // Then
    scenario.handle().blockLast();
    //    StepVerifier.create(handle)
    //                .expectNextCount(numOfAccounts * 5L + 100)
    //                .expectTimeout(Duration.ofMillis(5000))
    //                .verify();
  }

  @Disabled("For debugging purposes only")
  @ParameterizedTest(name = "Using {0} accounts")
  @ValueSource(ints = {1, 10, 100, 1000, 10_000, 100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 1000_000})
  void performance(int numOfAccounts) {
    // Given
    var commandRoute = new CommandRoute(toCommandTopic(numOfAccounts, randomPostFix), 0);
    var eventRoute = new EventRoute(toEventTopic(numOfAccounts, randomPostFix), 0);
    System.out.println(commandRoute + " : " + eventRoute);
    var pipeline = createPipeline(commandRoute, eventRoute);

    // When
    var scenario = new PerformanceScenario(data, pipeline, numOfAccounts);

    // Then
    StepVerifier.create(scenario.handle())
                .expectNextCount(numOfAccounts * 5L)
                .expectTimeout(Duration.ofMillis(1000))
                .verify();
  }

  @Disabled("Manual check")
  @Test
  void manualCheck() {
    // Given
    int numOfAccounts = 20;
    int randomPostFix = 232;
    var commandRoute = new CommandRoute(toCommandTopic(numOfAccounts, randomPostFix), 0);
    var eventRoute = new EventRoute(toEventTopic(numOfAccounts, randomPostFix), 0);
    System.out.println(commandRoute + " : " + eventRoute);
    var pipeline = createPipeline(commandRoute, eventRoute);

    // When
    var commands = pipeline.subToCommands().doOnNext(System.out::println);
    var events = pipeline.subToEvents().doOnNext(System.out::println);

    // Then
    //    StepVerifier.create(commands).expectNextCount(numOfAccounts * 5L).expectTimeout(Duration.ofMillis(500)).verify();
    StepVerifier.create(events).expectNextCount(10).verifyComplete();
  }

  private String toCommandTopic(int numOfAccounts, int random) {
    return "%d-%s-%d".formatted(numOfAccounts, commandsTopicPrefix, random);
  }

  private String toEventTopic(int numOfAccounts, int random) {
    return "%d-%s-%d".formatted(numOfAccounts, eventsTopicPrefix, random);
  }

  private PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline(CommandRoute commandRoute,
                                                                                  EventRoute eventRoute) {
    var commandConfig = createConfig(commandRoute.name(), commandRoute.totalPartitions());
    var eventConfig = createConfig(eventRoute.name(), eventRoute.totalPartitions());
    var natsConfig = new NatsConfig(NATS_URL, commandConfig.addAll(eventConfig));

    try {
      // Streams
      var commandStream = new NatsCommandStream<>(natsConfig, AccountCommand.class, transformer);
      var eventStream = new NatsEventStream<>(natsConfig, AccountEvent.class, transformer);

      // Pipeline
      return Infra.createPipeline(data.idSupplier,
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
