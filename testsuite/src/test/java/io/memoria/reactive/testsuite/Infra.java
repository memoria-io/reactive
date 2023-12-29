package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandRepo;
import io.memoria.reactive.eventsourcing.stream.CommandRoute;
import io.memoria.reactive.eventsourcing.stream.EventRepo;
import io.memoria.reactive.eventsourcing.stream.EventRoute;
import io.memoria.reactive.kafka.KafkaCommandRepo;
import io.memoria.reactive.kafka.KafkaEventRepo;
import io.memoria.reactive.nats.NatsCommandRepo;
import io.memoria.reactive.nats.NatsEventRepo;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.JetStreamApiException;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;

import static io.memoria.reactive.nats.NatsUtils.defaultStreamConfig;

public class Infra {

  public final String NATS_URL = "nats://localhost:4222";
  public final SerializableTransformer transformer = new SerializableTransformer();
  public final String groupId;

  public Infra(String groupId) {
    this.groupId = groupId;
  }

  public PartitionPipeline kafkaPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    var commandRepo = new KafkaCommandRepo(kafkaProducerConfigs(), kafkaConsumerConfigs(), commandRoute, transformer);
    var eventRepo = new KafkaEventRepo(kafkaProducerConfigs(),
                                       kafkaConsumerConfigs(),
                                       eventRoute,
                                       Duration.ofMillis(1000),
                                       transformer);
    return new PartitionPipeline(domain, commandRepo, eventRepo);
  }

  public PartitionPipeline natsPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    try {
      var nc = NatsUtils.createConnection(NATS_URL);
      var commandRepo = new NatsCommandRepo(nc, commandRoute, transformer);
      var eventRepo = new NatsEventRepo(nc, eventRoute, transformer);

      NatsUtils.createOrUpdateStream(nc.jetStreamManagement(),
                                     defaultStreamConfig(eventRoute.topic(), 1).build());
      NatsUtils.createOrUpdateStream(nc.jetStreamManagement(),
                                     defaultStreamConfig(commandRoute.topic(), 1).build());

      return new PartitionPipeline(domain, commandRepo, eventRepo);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  public PartitionPipeline inMemoryPipeline(Domain domain) {
    var commandRepo = CommandRepo.inMemory();
    var eventRepo = EventRepo.inMemory();
    return new PartitionPipeline(domain, commandRepo, eventRepo);
  }

  public Map<String, Object> kafkaConsumerConfigs() {
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
                      groupId);
  }

  public Map<String, Object> kafkaProducerConfigs() {
    return HashMap.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                      "localhost:9092",
                      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                      false,
                      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class,
                      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      StringSerializer.class);
  }
}
