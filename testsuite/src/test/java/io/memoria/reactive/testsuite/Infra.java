package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.memoria.reactive.kafka.KafkaMsgStream;
import io.memoria.reactive.nats.NatsMsgStream;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.JetStreamApiException;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Stream;

import static io.memoria.reactive.nats.NatsUtils.defaultStreamConfig;

public class Infra {
  public final String NATS_URL = "nats://localhost:4222";
  public final SerializableTransformer transformer = new SerializableTransformer();
  public final String groupId;

  public Infra(String groupId) {
    this.groupId = groupId;
  }

  public Stream<Arguments> pipelines(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    var inMemory = inMemoryPipeline(domain, commandRoute, eventRoute);
    var kafka = kafkaPipeline(domain, commandRoute, eventRoute);
    var nats = natsPipeline(domain, commandRoute, eventRoute);
    return Stream.of(Arguments.of(Named.of("In memory", inMemory)),
                     Arguments.of(Named.of("Kafka", kafka)),
                     Arguments.of(Named.of("Nats", nats)));
  }

  private PartitionPipeline kafkaPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    var repo = new KafkaMsgStream(kafkaProducerConfigs(), kafkaConsumerConfigs(), Duration.ofMillis(1000));
    return new PartitionPipeline(domain, repo, commandRoute, eventRoute, new SerializableTransformer());
  }

  private PartitionPipeline natsPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    try {
      var nc = NatsUtils.createConnection(NATS_URL);
      var repo = new NatsMsgStream(nc, eventRoute, transformer);

      NatsUtils.createOrUpdateStream(nc.jetStreamManagement(), defaultStreamConfig(eventRoute.topic(), 1).build());
      NatsUtils.createOrUpdateStream(nc.jetStreamManagement(), defaultStreamConfig(commandRoute.topic(), 1).build());

      return new PartitionPipeline(domain, repo, commandRoute, eventRoute, new SerializableTransformer());
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  private PartitionPipeline inMemoryPipeline(Domain domain, CommandRoute commandRoute, EventRoute eventRoute) {
    var repo = MsgStream.inMemory();
    return new PartitionPipeline(domain, repo, commandRoute, eventRoute, new SerializableTransformer());
  }

  private Map<String, Object> kafkaConsumerConfigs() {
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

  private Map<String, Object> kafkaProducerConfigs() {
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
