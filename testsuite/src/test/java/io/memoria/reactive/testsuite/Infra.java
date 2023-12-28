package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandRepo;
import io.memoria.reactive.eventsourcing.stream.EventRepo;
import io.memoria.reactive.kafka.KafkaCommandRepo;
import io.memoria.reactive.kafka.KafkaEventRepo;
import io.memoria.reactive.nats.NatsCommandRepo;
import io.memoria.reactive.nats.NatsEventRepo;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;

public class Infra {

  public final String NATS_URL = "nats://localhost:4222";
  public final SerializableTransformer transformer = new SerializableTransformer();

  public final CommandRoute commandRoute;
  public final EventRoute eventRoute;
  public final String groupId;

  public Infra(CommandRoute commandRoute, EventRoute eventRoute, String groupId) {
    this.commandRoute = commandRoute;
    this.eventRoute = eventRoute;
    this.groupId = groupId;
  }

  public PartitionPipeline kafkaPipeline(Domain domain) {
    var commandRepo = new KafkaCommandRepo(kafkaProducerConfigs(),
                                           kafkaConsumerConfigs(),
                                           commandRoute.topic(),
                                           commandRoute.totalPartitions(),
                                           transformer);
    var eventRepo = new KafkaEventRepo(kafkaProducerConfigs(),
                                       kafkaConsumerConfigs(),
                                       eventRoute.topic(),
                                       eventRoute.totalPartitions(),
                                       Duration.ofMillis(1000),
                                       transformer);
    return new PartitionPipeline(domain, commandRepo, eventRepo, eventRoute);
  }

  public PartitionPipeline natsPipeline(Domain domain) {
    try {
      var nc = NatsUtils.createConnection(NATS_URL);
      JetStreamManagement jsManagement = nc.jetStreamManagement();
      var commandRepo = new NatsCommandRepo(nc, commandRoute.topic(), commandRoute.totalPartitions(), transformer);
      var eventRepo = new NatsEventRepo(nc, eventRoute.topic(), eventRoute.totalPartitions(), transformer);
      NatsUtils.createOrUpdateStream(jsManagement, commandRoute.topic(), 1);
      NatsUtils.createOrUpdateStream(jsManagement, eventRoute.topic(), 1);
      return new PartitionPipeline(domain, commandRepo, eventRepo, eventRoute);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  public PartitionPipeline inMemoryPipeline(Domain domain) {
    var commandRepo = CommandRepo.inMemory();
    var eventRepo = EventRepo.inMemory(eventRoute.totalPartitions());
    return new PartitionPipeline(domain, commandRepo, eventRepo, eventRoute);
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
