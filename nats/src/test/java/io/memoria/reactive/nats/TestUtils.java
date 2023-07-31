package io.memoria.reactive.nats;

import io.memoria.reactive.eventsourcing.pipeline.partition.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.nats.eventsourcing.stream.NatsCommandStream;
import io.memoria.reactive.nats.eventsourcing.stream.NatsEventStream;
import io.memoria.reactive.testsuite.TestsuiteUtils;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingInfra;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StorageType;

import java.io.IOException;
import java.time.Duration;

import static io.memoria.reactive.testsuite.TestsuiteUtils.SCHEDULER;
import static io.memoria.reactive.testsuite.TestsuiteUtils.TRANSFORMER;

public class TestUtils {
  public static final String NATS_URL = "nats://localhost:4222";
  public static final BankingData DATA = BankingData.ofUUID();
  public static final NatsConfig NATS_CONFIG = NatsConfig.appendOnly(NATS_URL,
                                                                     StorageType.File,
                                                                     1,
                                                                     1000,
                                                                     Duration.ofMillis(100),
                                                                     Duration.ofMillis(300));

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline() {
    try {
      var commandStream = new NatsCommandStream<>(NATS_CONFIG, AccountCommand.class, TRANSFORMER, SCHEDULER);
      var eventStream = new NatsEventStream<>(NATS_CONFIG, AccountEvent.class, TRANSFORMER, SCHEDULER);
      var commandRoute = new CommandRoute(TestsuiteUtils.topicName("commands"), 0);
      var eventRoute = new EventRoute(TestsuiteUtils.topicName("events"), 0);
      System.out.printf("Creating %s %n", commandRoute);
      System.out.printf("Creating %s %n", eventRoute);
      NatsUtils.createOrUpdateTopic(NATS_CONFIG, commandRoute.topicName(), commandRoute.totalPartitions());
      NatsUtils.createOrUpdateTopic(NATS_CONFIG, eventRoute.topicName(), eventRoute.totalPartitions());
      return BankingInfra.createPipeline(DATA.idSupplier,
                                         DATA.timeSupplier,
                                         commandStream,
                                         commandRoute,
                                         eventStream,
                                         eventRoute);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  private TestUtils() {}
}
