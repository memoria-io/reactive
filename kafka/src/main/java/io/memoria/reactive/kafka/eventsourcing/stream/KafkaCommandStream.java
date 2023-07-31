package io.memoria.reactive.kafka.eventsourcing.stream;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
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

import static java.util.Collections.singleton;

public class KafkaCommandStream<C extends Command> implements CommandStream<C> {
  public final Map<String, Object> producerConfig;
  public final Map<String, Object> consumerConfig;
  public final Class<C> cClass;
  public final TextTransformer transformer;
  private final KafkaSender<String, String> sender;

  public KafkaCommandStream(Map<String, Object> producerConfig,
                            Map<String, Object> consumerConfig,
                            Class<C> cClass,
                            TextTransformer transformer) {
    this.producerConfig = producerConfig;
    this.consumerConfig = consumerConfig;
    this.cClass = cClass;
    this.transformer = transformer;
    this.sender = KafkaUtils.createSender(producerConfig);
  }

  @Override
  public Mono<C> pub(String topic, int partition, C c) {
    return this.sender.send(this.toRecord(topic, partition, c)).map(SenderResult::correlationMetadata).single();
  }

  @Override
  public Flux<C> sub(String topic, int partition) {
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

  private Mono<SenderRecord<String, String, C>> toRecord(String topic, int partition, C cmd) {
    return ReactorUtils.tryToMono(() -> transformer.serialize(cmd))
                       .map(payload -> toRecord(topic, partition, cmd, payload));
  }

  private SenderRecord<String, String, C> toRecord(String topic, int partition, C cmd, String payload) {
    return SenderRecord.create(topic, partition, null, cmd.commandId().id().value(), payload, cmd);
  }

  public Mono<C> toMsg(ConsumerRecord<String, String> rec) {
    return ReactorUtils.tryToMono(() -> transformer.deserialize(rec.value(), cClass));
  }
}
