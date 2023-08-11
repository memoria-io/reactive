package io.memoria.reactive.testsuite.kafka;

import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.kafka.KafkaMsgStream;
import io.memoria.reactive.testsuite.Data;
import io.memoria.reactive.testsuite.Infra;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;

import static io.memoria.reactive.testsuite.Utils.TRANSFORMER;
import static io.memoria.reactive.testsuite.Utils.topicName;

class TestUtils {
  public static final Duration kafkaTimeout = Duration.ofMillis(500);
  public static final Data DATA = Data.ofUUID();

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
    return Infra.createPipeline(DATA.idSupplier,
                                DATA.timeSupplier,
                                commandStream,
                                commandRoute,
                                eventStream,
                                eventRoute);

  }
}