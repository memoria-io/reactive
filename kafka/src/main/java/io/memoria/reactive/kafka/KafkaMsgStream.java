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
  private final Duration lastEventTimeout;

  public KafkaMsgStream(Map<String, Object> producerConfig,
                        Map<String, Object> consumerConfig,
                        Duration lastEventTimeout) {
    this.consumerConfig = consumerConfig;
    this.lastEventTimeout = lastEventTimeout;
    var senderOptions = SenderOptions.<String, String>create(producerConfig.toJavaMap());
    this.producer = KafkaSender.create(senderOptions);
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    return Mono.fromCallable(() -> KafkaUtils.lastKey(topic, partition, lastEventTimeout, consumerConfig))
               .flatMap(ReactorUtils::optionToMono)
               .map(this::toMsg);
  }

  @Override
  public Mono<Msg> pub(String topic, int partition, Msg msg) {
    return producer.send(Mono.fromCallable(() -> toRecord(topic, partition, msg)))
                   .map(SenderResult::correlationMetadata)
                   .single();
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    return KafkaUtils.subscribe(topic, partition, consumerConfig).map(this::toMsg);
  }

  private SenderRecord<String, String, Msg> toRecord(String topic, int partition, Msg msg) {
    return SenderRecord.create(topic, partition, null, msg.key(), msg.value(), msg);
  }

  private Msg toMsg(ConsumerRecord<String, String> record) {
    return new Msg(record.key(), record.value());
  }
}
