package io.memoria.reactive.kafka;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.time.Duration;

public class KafkaUtils {
  private KafkaUtils() {}

  public static long topicSize(String topic, int partition, Map<String, Object> conf) {
    try (var consumer = new KafkaConsumer<String, String>(conf.toJavaMap())) {
      var tp = new TopicPartition(topic, partition);
      var tpCol = List.of(tp).toJavaList();
      consumer.assign(tpCol);
      consumer.seekToEnd(tpCol);
      return consumer.position(tp);
    }
  }

  public static Option<String> lastKey(String topic, int partition, Duration timeout, Map<String, Object> conf) {
    try (var consumer = new KafkaConsumer<String, String>(conf.toJavaMap())) {
      var tp = new TopicPartition(topic, partition);
      var tpCol = List.of(tp).toJavaList();
      consumer.assign(tpCol);
      consumer.seekToEnd(tpCol);
      var position = consumer.position(tp);
      if (position < 1)
        return Option.none();
      long startIndex = position - 1;
      consumer.seek(tp, startIndex);
      var records = consumer.poll(timeout).records(tp);
      var size = records.size();
      if (size > 0) {
        return Option.of(records.get(size - 1).key());
      } else {
        return Option.none();
      }
    }
  }

  public static KafkaSender<String, String> createSender(Map<String, Object> config) {
    var senderOptions = SenderOptions.<String, String>create(config.toJavaMap());
    return KafkaSender.create(senderOptions);
  }
}
