package io.memoria.reactive.kafka.message.stream;

import io.memoria.reactive.core.message.stream.ESMsg;
import io.memoria.reactive.core.message.stream.ESMsgStream;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.kafka.KafkaUtils;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;

import static java.util.Collections.singleton;

public class KafkaESMsgStream implements ESMsgStream {
  public final Map<String, Object> producerConfig;
  public final Map<String, Object> consumerConfig;
  private final KafkaSender<String, String> sender;
  private final Duration timeout;

  public KafkaESMsgStream(Map<String, Object> producerConfig, Map<String, Object> consumerConfig) {
    this(producerConfig, consumerConfig, Duration.ofMillis(500));
  }

  public KafkaESMsgStream(Map<String, Object> producerConfig, Map<String, Object> consumerConfig, Duration timeout) {
    this.producerConfig = producerConfig;
    this.consumerConfig = consumerConfig;
    this.sender = KafkaUtils.createSender(producerConfig);
    this.timeout = timeout;
  }

  @Override
  public Mono<String> last(String topic, int partition) {
    return Mono.fromCallable(() -> KafkaUtils.lastKey(topic, partition, timeout, consumerConfig))
               .flatMap(ReactorUtils::optionToMono);
  }

  @Override
  public Mono<ESMsg> pub(String topic, int partition, ESMsg msg) {
    return this.sender.send(Mono.fromCallable(() -> toRecord(topic, partition, msg)))
                      .map(SenderResult::correlationMetadata)
                      .single();
  }

  @Override
  public Flux<ESMsg> sub(String topic, int partition) {
    return receive(topic, partition).map(KafkaESMsgStream::toMsg);
  }

  Flux<ReceiverRecord<String, String>> receive(String topic, int partition) {
    var tp = new TopicPartition(topic, partition);
    var receiverOptions = ReceiverOptions.<String, String>create(consumerConfig.toJavaMap())
                                         .subscription(singleton(topic))
                                         .addAssignListener(partitions -> partitions.forEach(p -> p.seek(0)))
                                         .assignment(singleton(tp));
    return KafkaReceiver.create(receiverOptions).receive();
  }

  static SenderRecord<String, String, ESMsg> toRecord(String topic, int partition, ESMsg esMsg) {
    return SenderRecord.create(topic, partition, null, esMsg.key(), esMsg.value(), esMsg);
  }

  static ESMsg toMsg(ConsumerRecord<String, String> rec) {
    return new ESMsg(rec.key(), rec.value());
  }
}
