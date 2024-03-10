package io.memoria.reactive.kafka;

import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.stream.Msg;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;

public class KafkaMsgStream implements MsgStream {
  private final KafkaSender<String, String> producer;
  private final Map<String, Object> consumerConfig;
  private final Duration timeout;

  public KafkaMsgStream(Map<String, Object> producerConfig, Map<String, Object> consumerConfig, Duration timeout) {
    this.consumerConfig = consumerConfig;
    this.timeout = timeout;
    var senderOptions = SenderOptions.<String, String>create(producerConfig.toJavaMap());
    this.producer = KafkaSender.create(senderOptions);
  }

  @Override
  public Mono<Msg> pub(Msg msg) {
    return producer.send(Mono.fromCallable(() -> toRecord(msg))).map(SenderResult::correlationMetadata).single();
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    return KafkaUtils.subscribe(topic, partition, consumerConfig).map(rec -> toMsg(topic, partition, rec));
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    return Mono.fromCallable(() -> KafkaUtils.lastKey(topic, partition, timeout, consumerConfig))
               .flatMap(ReactorUtils::optionToMono)
               .map(rec -> toMsg(topic, partition, rec));
  }

  private SenderRecord<String, String, Msg> toRecord(Msg msg) {
    return SenderRecord.create(msg.topic(), msg.partition(), null, msg.key(), msg.value(), msg);
  }

  private Msg toMsg(String topic, int partition, ConsumerRecord<String, String> rec) {
    return new Msg(topic, partition, rec.key(), rec.value());
  }
}
