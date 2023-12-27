package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;

public class Configs {
  public final String NATS_URL = "nats://localhost:4222";
  public final Duration TIMEOUT = Duration.ofMillis(500);
  public final SerializableTransformer transformer = new SerializableTransformer();

  public final String commandsTopic;
  public final String eventsTopic;
  public final String groupId;
  public final int totalCommandPartitions;
  public final int totalEventPartitions;
  public final int eventPartition;

  public Configs(String postfix, int totalCommandPartitions, int totalEventPartitions, int eventPartition) {
    this.commandsTopic = "commands_" + postfix;
    this.eventsTopic = "events_" + postfix;
    this.groupId = "consumer_group_" + postfix;
    this.totalCommandPartitions = totalCommandPartitions;
    this.totalEventPartitions = totalEventPartitions;
    this.eventPartition = eventPartition;
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
