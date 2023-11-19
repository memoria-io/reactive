package io.memoria.reactive.testsuite;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.reactive.core.stream.MsgStream;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.kafka.KafkaMsgStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsMsgStream;
import io.memoria.reactive.testsuite.command.AccountCommand;
import io.memoria.reactive.testsuite.event.AccountEvent;
import io.memoria.reactive.testsuite.state.Account;
import io.nats.client.api.StorageType;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.control.Try;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.function.Supplier;

import static io.memoria.reactive.testsuite.Infra.StreamType.KAFKA;
import static io.memoria.reactive.testsuite.Infra.StreamType.MEMORY;
import static io.memoria.reactive.testsuite.Infra.StreamType.NATS;

public class Infra {
  private static final Logger log = LoggerFactory.getLogger(Infra.class.getName());
  public static final Duration TIMEOUT = Duration.ofMillis(500);

  public enum StreamType {
    KAFKA,
    NATS,
    MEMORY
  }

  public static final String NATS_URL = "nats://localhost:4222";
  public static final NatsConfig NATS_CONFIG = NatsConfig.appendOnly(NATS_URL,
                                                                     StorageType.File,
                                                                     1,
                                                                     100,
                                                                     Duration.ofMillis(100),
                                                                     Duration.ofMillis(300));
  public static final MsgStream inMemoryStream = msgStream(MEMORY).get();
  public static final MsgStream kafkaStream = msgStream(KAFKA).get();
  public static final MsgStream natsStream = msgStream(NATS).get();

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> pipeline(Supplier<Id> idSupplier,
                                                                                  Supplier<Long> timeSupplier,
                                                                                  CommandRoute commandRoute,
                                                                                  EventRoute eventRoute,
                                                                                  MsgStream msgStream,
                                                                                  boolean startupSaga) {
    // Stream
    var transformer = new SerializableTransformer();
    var commandStream = CommandStream.msgStream(msgStream, AccountCommand.class, transformer);
    var eventStream = EventStream.msgStream(msgStream, AccountEvent.class, transformer);

    // Pipeline
    var domain = domain(idSupplier, timeSupplier);
    return new PartitionPipeline<>(domain, commandStream, commandRoute, eventStream, eventRoute, startupSaga);

  }

  public static Try<MsgStream> msgStream(StreamType streamType) {
    return Try.of(() -> switch (streamType) {
      case KAFKA -> new KafkaMsgStream(kafkaProducerConfigs(), kafkaConsumerConfigs(), Duration.ofMillis(500));
      case NATS -> new NatsMsgStream(NATS_CONFIG, Schedulers.boundedElastic());
      case MEMORY -> MsgStream.inMemory();
    });
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

  public static String randomTopicName(String postfix) {
    return STR. "topic\{ System.currentTimeMillis() }_\{ postfix }" ;
  }

  public static void printRates(String methodName, long start, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - start;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec(msgCount, totalElapsed));
  }

  public static Map<String, Object> kafkaConsumerConfigs() {
    return HashMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      "localhost:9092",
                      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                      false,
                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                      "earliest",
                      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class,
                      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class,
                      ConsumerConfig.GROUP_ID_CONFIG,
                      "some_group_id1");
  }

  public static Map<String, Object> kafkaProducerConfigs() {
    return HashMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      "localhost:9092",
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                      false,
                      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class,
                      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class);
  }

  private static double eventsPerSec(long msgCount, long totalElapsedMillis) {
    return msgCount / (totalElapsedMillis / 1000d);
  }

  private Infra() {}
}
