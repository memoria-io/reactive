package io.memoria.reactive.kafka;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.control.Option;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KafkaUtils {
  private static final Logger log = LoggerFactory.getLogger(KafkaUtils.class.getName());

  private KafkaUtils() {}

  public static void createTopic(Map<String, Object> adminConfig, String topic, int numOfPartition, Duration timeout)
          throws ExecutionException, InterruptedException, TimeoutException {
    try (var client = KafkaAdminClient.create(adminConfig.toJavaMap())) {
      var tp = new NewTopic(topic, Optional.of(numOfPartition), Optional.empty());
      client.createTopics(List.of(tp).toJavaList()).all().get(timeout.toMillis(), MILLISECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException ex) {
        log.warn(ex.getMessage());
      } else {
        throw e;
      }
    }
  }

  public static long topicSize(String topic, int partition, Map<String, Object> conf) {
    try (var consumer = new KafkaConsumer<String, String>(conf.toJavaMap())) {
      var tp = new TopicPartition(topic, partition);
      var tpCol = List.of(tp).toJavaList();
      consumer.assign(tpCol);
      consumer.seekToEnd(tpCol);
      return consumer.position(tp);
    }
  }

  public static Option<ConsumerRecord<String, String>> lastKey(String topic,
                                                               int partition,
                                                               Duration timeout,
                                                               Map<String, Object> conf) {
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
        return Option.of(records.get(size - 1));
      } else {
        return Option.none();
      }
    }
  }

  public static ReceiverOptions<String, String> receiveOptions(String topic,
                                                               int partition,
                                                               Map<String, Object> consumerConfig) {
    var tp = singleton(new TopicPartition(topic, partition));
    return ReceiverOptions.<String, String>create(consumerConfig.toJavaMap())
                          .assignment(tp)
                          .addAssignListener(p -> p.forEach(r -> r.seek(0)));
  }

  public static Flux<ReceiverRecord<String, String>> subscribe(String topic,
                                                               int partition,
                                                               Map<String, Object> consumerConfig) {
    var options = receiveOptions(topic, partition, consumerConfig);
    return KafkaReceiver.create(options).receive();
  }
}
