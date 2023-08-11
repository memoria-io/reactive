package io.memoria.reactive.testsuite;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountDecider;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountEvolver;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountSaga;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.core.stream.MsgStream;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.kafka.KafkaMsgStream;
import io.memoria.reactive.nats.NatsMsgStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Supplier;

import static io.memoria.reactive.testsuite.Config.NATS_CONFIG;
import static io.memoria.reactive.testsuite.Config.kafkaConsumerConfigs;
import static io.memoria.reactive.testsuite.Config.kafkaProducerConfigs;

public class Infra {
  private static final Logger log = LoggerFactory.getLogger(Infra.class.getName());
  public static final Duration TIMEOUT = Duration.ofMillis(500);

  public enum StreamType {
    KAFKA,
    NATS,
    MEMORY
  }

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline(Supplier<Id> idSupplier,
                                                                                  Supplier<Long> timeSupplier,
                                                                                  StreamType streamType,
                                                                                  CommandRoute commandRoute,
                                                                                  EventRoute eventRoute)
          throws IOException, InterruptedException {

    var transformer = new SerializableTransformer();

    // Stream
    var msgStream = msgStream(streamType);
    var commandStream = CommandStream.msgStream(msgStream, AccountCommand.class, transformer);
    var eventStream = EventStream.msgStream(msgStream, AccountEvent.class, transformer);

    // Pipeline
    var domain = domain(idSupplier, timeSupplier);
    return new PartitionPipeline<>(domain, commandStream, commandRoute, eventStream, eventRoute);
  }

  public static MsgStream msgStream(StreamType streamType) throws IOException, InterruptedException {
    return switch (streamType) {
      case KAFKA -> new KafkaMsgStream(kafkaProducerConfigs(), kafkaConsumerConfigs(), Duration.ofMillis(500));
      case NATS -> new NatsMsgStream(NATS_CONFIG, Schedulers.boundedElastic());
      case MEMORY -> MsgStream.inMemory();
    };
  }

  public static Domain<Account, AccountCommand, AccountEvent> domain(Supplier<Id> idSupplier,
                                                                     Supplier<Long> timeSupplier) {
    return new Domain<>(Account.class,
                        AccountCommand.class,
                        AccountEvent.class,
                        new AccountDecider(idSupplier, timeSupplier),
                        new AccountEvolver(),
                        new AccountSaga(idSupplier, timeSupplier));
  }

  public static String topicName(String postfix) {
    return "topic%d_%s".formatted(System.currentTimeMillis(), postfix);
  }

  public static void printRates(String methodName, long now, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - now;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec(msgCount, totalElapsed));
  }

  private static double eventsPerSec(long msgCount, long totalElapsed) {
    return msgCount / (totalElapsed / 1000d);
  }

  private Infra() {}
}
