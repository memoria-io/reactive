package io.memoria.reactive.kafka;

import io.vavr.collection.HashMap;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class KafkaMsgStreamTest {
  public static final Duration kafkaTimeout = Duration.ofMillis(500);

  @Test
  void topicSize() {
    var conf = consumerConfigs();
    var size = Utils.topicSize("some_topic", 0, conf);
    Assertions.assertEquals(0, size);
  }

  @Test
  void lastKey() {
    var conf = consumerConfigs();
    var keyOpt = Utils.lastKey("some_topic", 0, Duration.ofMillis(300), conf);
    Assertions.assertEquals(Option.none(), keyOpt);
  }

  private static Map<String, Object> consumerConfigs() {
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

  private static Map<String, Object> producerConfigs() {
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
