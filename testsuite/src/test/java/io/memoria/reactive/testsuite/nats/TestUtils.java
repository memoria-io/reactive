package io.memoria.reactive.testsuite.nats;

import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsMsgStream;
import io.memoria.reactive.nats.Utils;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingInfra;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StorageType;

import java.io.IOException;
import java.time.Duration;

import static io.memoria.reactive.testsuite.Utils.SCHEDULER;
import static io.memoria.reactive.testsuite.Utils.TRANSFORMER;

class TestUtils {
  public static final String NATS_URL = "nats://localhost:4222";
  public static final BankingData DATA = BankingData.ofSerial();
  public static final NatsConfig NATS_CONFIG = NatsConfig.appendOnly(NATS_URL,
                                                                     StorageType.File,
                                                                     1,
                                                                     1000,
                                                                     Duration.ofMillis(100),
                                                                     Duration.ofMillis(300));

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline() {
    try {
      var msgStream = new NatsMsgStream(NATS_CONFIG, SCHEDULER);
      var commandStream = CommandStream.msgStream(msgStream, AccountCommand.class, TRANSFORMER);
      var eventStream = EventStream.msgStream(msgStream, AccountEvent.class, TRANSFORMER);
      var commandRoute = new CommandRoute(io.memoria.reactive.testsuite.Utils.topicName("commands"), 0);
      var eventRoute = new EventRoute(io.memoria.reactive.testsuite.Utils.topicName("events"), 0);
      //      System.out.printf("Creating %s %n", commandRoute);
      //      System.out.printf("Creating %s %n", eventRoute);
      Utils.createOrUpdateTopic(NATS_CONFIG, commandRoute.topicName(), commandRoute.totalPartitions());
      Utils.createOrUpdateTopic(NATS_CONFIG, eventRoute.topicName(), eventRoute.totalPartitions());
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
