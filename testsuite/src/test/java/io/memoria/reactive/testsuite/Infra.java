package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.memoria.reactive.kafka.KafkaMsgStream;
import io.memoria.reactive.kafka.KafkaUtils;
import io.memoria.reactive.nats.NatsMsgStream;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.memoria.reactive.nats.NatsUtils.defaultStreamConfig;

public class Infra {
  private final String KAFKA_URL;
  private final String groupId;
  private final Duration timeout;
  private final Connection nc;
  private final SerializableTransformer transformer;
  public final MsgStream inMemoryStream;
  public final MsgStream kafkaMsgStream;
  public final MsgStream natsStream;

  public Infra(String groupId) {
    try {
      // config
      this.groupId = groupId;
      this.KAFKA_URL = "localhost:9092";
      this.timeout = Duration.ofMillis(1000);
      this.nc = NatsUtils.createConnection("nats://localhost:4222");
      this.transformer = new SerializableTransformer();
      // streams
      this.inMemoryStream = MsgStream.inMemory();
      this.kafkaMsgStream = new KafkaMsgStream(kafkaProducerConfigs(), kafkaConsumerConfigs(), timeout);
      this.natsStream = new NatsMsgStream(nc);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Tuple2<CommandRoute, EventRoute>> getRoutes(int x) {
    var cTopic = "commands" + System.currentTimeMillis();
    var eTopic = "events" + System.currentTimeMillis();
    return List.range(0, x).map(i -> Tuple.of(new CommandRoute(cTopic, i, x), new EventRoute(eTopic, i, x)));
  }

  public void createKafkaTopics(String topic, int partitions) {
    try {
      KafkaUtils.createTopic(kafkaAdminConfigs(), topic, partitions, timeout);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public void createNatsTopics(String topic) {
    try {
      NatsUtils.createOrUpdateStream(nc.jetStreamManagement(), defaultStreamConfig(topic, 1).build());
    } catch (IOException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  public PartitionPipeline inMemoryPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    return new PartitionPipeline(domain, commandRoute, eventRoute, inMemoryStream, transformer);
  }

  public PartitionPipeline natsPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    return new PartitionPipeline(domain, commandRoute, eventRoute, natsStream, transformer);
  }

  public PartitionPipeline kafkaPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    return new PartitionPipeline(domain, commandRoute, eventRoute, kafkaMsgStream, transformer);
  }

  public Map<String, Object> kafkaAdminConfigs() {
    return HashMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
  }

  /**
   * Acknowledgement only means committing the offset in Kafka for a certain consumer group, unlike nats which stops
   * consumption if explicit ack is set.
   * <a href="https://stackoverflow.com/a/59846269/263215"> kafka committing stackoverflow answer</a>
   */
  public Map<String, Object> kafkaConsumerConfigs() {
    return HashMap.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      KAFKA_URL,
                      ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                      10,
                      ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                      IsolationLevel.READ_COMMITTED.toString().toLowerCase(),
                      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                      true,
                      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                      "earliest",
                      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class,
                      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class,
                      ConsumerConfig.GROUP_ID_CONFIG,
                      groupId);
  }

  public Map<String, Object> kafkaProducerConfigs() {
    return HashMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      KAFKA_URL,
                      //                      ProducerConfig.ACKS_CONFIG,
                      //                      "0",
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                      false,
                      ProducerConfig.MAX_BLOCK_MS_CONFIG,
                      2000,
                      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class,
                      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class);
  }
}
