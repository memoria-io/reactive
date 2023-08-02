package io.memoria.reactive.kafka;

import io.memoria.reactive.eventsourcing.pipeline.partition.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingData;
import io.memoria.reactive.testsuite.eventsourcing.banking.BankingInfra;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;

import static io.memoria.reactive.testsuite.TestsuiteUtils.TRANSFORMER;
import static io.memoria.reactive.testsuite.TestsuiteUtils.topicName;

public class TestUtils {
  public static final Duration kafkaTimeout = Duration.ofMillis(500);
  public static final BankingData DATA = BankingData.ofUUID();

  private TestUtils() {}

  public static Map<String, Object> consumerConfigs() {
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

  public static Map<String, Object> producerConfigs() {
    return HashMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      "localhost:9092",
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                      false,
                      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class,
                      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class);
  }

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline() {
    var msgStream = new KafkaMsgStream(producerConfigs(), consumerConfigs(), Duration.ofMillis(500));
    var commandStream = CommandStream.msgStream(msgStream, AccountCommand.class, TRANSFORMER);
    var eventStream = EventStream.msgStream(msgStream, AccountEvent.class, TRANSFORMER);
    var commandRoute = new CommandRoute(topicName("commands"), 0);
    var eventRoute = new EventRoute(topicName("events"), 0);
    return BankingInfra.createPipeline(DATA.idSupplier,
                                       DATA.timeSupplier,
                                       commandStream,
                                       commandRoute,
                                       eventStream,
                                       eventRoute);

  }
}
