package io.memoria.reactive.kafka.eventsourcing;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.stream.EventStream;
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

public class KafkaEventStream<E extends Event> implements EventStream<E> {
  public final Map<String, Object> producerConfig;
  public final Map<String, Object> consumerConfig;
  public final Class<E> cClass;
  public final TextTransformer transformer;
  private final KafkaSender<String, String> sender;
  private final Duration timeout;

  public KafkaEventStream(Map<String, Object> producerConfig,
                          Map<String, Object> consumerConfig,
                          Class<E> cClass,
                          TextTransformer transformer,
                          KafkaSender<String, String> sender,
                          Duration timeout) {
    this.producerConfig = producerConfig;
    this.consumerConfig = consumerConfig;
    this.cClass = cClass;
    this.transformer = transformer;
    this.sender = sender;
    this.timeout = timeout;
  }

  @Override
  public Mono<Id> last(String topic, int partition) {
    return Mono.fromCallable(() -> KafkaUtils.lastKey(topic, partition, timeout, consumerConfig))
               .flatMap(ReactorUtils::optionToMono)
               .map(Id::of);
  }

  @Override
  public Mono<E> pub(String topic, int partition, E c) {
    return this.sender.send(this.toRecord(topic, partition, c)).map(SenderResult::correlationMetadata).singleOrEmpty();
  }

  @Override
  public Flux<E> sub(String topic, int partition) {
    return receive(topic, partition).concatMap(this::toMsg);
  }

  private Flux<ReceiverRecord<String, String>> receive(String topic, int partition) {
    var tp = new TopicPartition(topic, partition);
    var receiverOptions = ReceiverOptions.<String, String>create(consumerConfig.toJavaMap())
                                         .subscription(singleton(topic))
                                         .addAssignListener(partitions -> partitions.forEach(p -> p.seek(0)))
                                         .assignment(singleton(tp));
    return KafkaReceiver.create(receiverOptions).receive();
  }

  private Mono<SenderRecord<String, String, E>> toRecord(String topic, int partition, E event) {
    return ReactorUtils.tryToMono(() -> transformer.serialize(event))
                       .map(payload -> toRecord(topic, partition, event, payload));
  }

  private SenderRecord<String, String, E> toRecord(String topic, int partition, E event, String payload) {
    return SenderRecord.create(topic, partition, null, event.commandId().value(), payload, event);
  }

  public Mono<E> toMsg(ConsumerRecord<String, String> rec) {
    return ReactorUtils.tryToMono(() -> transformer.deserialize(rec.value(), cClass));
  }
}
