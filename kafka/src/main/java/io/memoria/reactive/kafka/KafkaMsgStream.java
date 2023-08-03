package io.memoria.reactive.kafka;

import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.core.stream.MsgStream;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;

import static java.util.Collections.singleton;

public class KafkaMsgStream implements MsgStream {
  public final Map<String, Object> producerConfig;
  public final Map<String, Object> consumerConfig;
  private final Duration timeout;
  private final KafkaSender<String, String> sender;

  public KafkaMsgStream(Map<String, Object> producerConfig, Map<String, Object> consumerConfig, Duration timeout) {
    this.producerConfig = producerConfig;
    this.consumerConfig = consumerConfig;
    this.timeout = timeout;
    var senderOptions = SenderOptions.<String, String>create(producerConfig.toJavaMap());
    this.sender = KafkaSender.create(senderOptions);
  }

  @Override
  public Mono<Msg> pub(String topic, int partition, Msg msg) {
    return sender.send(Mono.fromCallable(() -> toRecord(topic, partition, msg)))
                 .map(SenderResult::correlationMetadata)
                 .single();
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    var tp = new TopicPartition(topic, partition);
    var receiverOptions = ReceiverOptions.<String, String>create(consumerConfig.toJavaMap())
                                         .subscription(singleton(topic))
                                         .addAssignListener(partitions -> partitions.forEach(p -> p.seek(0)))
                                         .assignment(singleton(tp));
    var receiver = KafkaReceiver.create(receiverOptions);
    return receiver.receive().map(KafkaMsgStream::toMsg);
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    return Mono.fromCallable(() -> KafkaUtils.lastKey(topic, partition, timeout, consumerConfig))
               .flatMap(ReactorUtils::optionToMono)
               .map(KafkaMsgStream::toMsg);
  }

  @Override
  public void close() {
    this.sender.close();
  }

  static SenderRecord<String, String, Msg> toRecord(String topic, int partition, Msg msg) {
    return SenderRecord.create(topic, partition, null, msg.key(), msg.value(), msg);
  }

  static Msg toMsg(ConsumerRecord<String, String> rec) {
    return new Msg(rec.key(), rec.value());
  }
}
