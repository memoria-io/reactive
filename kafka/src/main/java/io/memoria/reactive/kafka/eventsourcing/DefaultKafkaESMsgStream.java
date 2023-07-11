package io.memoria.reactive.kafka.eventsourcing;

import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.core.stream.ESMsg;
import io.vavr.collection.Map;
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

class DefaultKafkaESMsgStream implements KafkaESMsgStream {
  public final Map<String, Object> producerConfig;
  public final Map<String, Object> consumerConfig;
  private final KafkaSender<String, String> sender;
  private final Duration timeout;

  DefaultKafkaESMsgStream(Map<String, Object> producerConfig,
                          Map<String, Object> consumerConfig,
                          KafkaSender<String, String> sender) {
    this(producerConfig, consumerConfig, sender, Duration.ofMillis(500));
  }

  DefaultKafkaESMsgStream(Map<String, Object> producerConfig,
                          Map<String, Object> consumerConfig,
                          KafkaSender<String, String> sender,
                          Duration timeout) {
    this.sender = sender;
    this.producerConfig = producerConfig;
    this.consumerConfig = consumerConfig;
    this.timeout = timeout;
  }

  @Override
  public Mono<String> lastKey(String topic, int partition) {
    return Mono.fromCallable(() -> KafkaUtils.lastKey(topic, partition, timeout, consumerConfig))
               .flatMap(ReactorUtils::optionToMono);
  }

  @Override
  public Mono<ESMsg> pub(ESMsg msg) {
    var rec = this.toRecord(msg);
    return this.sender.send(Mono.just(rec)).map(SenderResult::correlationMetadata).singleOrEmpty();
  }

  @Override
  public Flux<ESMsg> sub(String topic, int partition) {
    return receive(topic, partition).map(KafkaUtils::toMsg);
  }

  private Flux<ReceiverRecord<String, String>> receive(String topic, int partition) {
    var tp = new TopicPartition(topic, partition);
    var receiverOptions = ReceiverOptions.<String, String>create(consumerConfig.toJavaMap())
                                         .subscription(singleton(topic))
                                         .addAssignListener(partitions -> partitions.forEach(p -> p.seek(0)))
                                         .assignment(singleton(tp));
    return KafkaReceiver.create(receiverOptions).receive();
  }

  private SenderRecord<String, String, ESMsg> toRecord(ESMsg esMsg) {
    return SenderRecord.create(esMsg.topic(), esMsg.partition(), null, esMsg.key(), esMsg.value(), esMsg);
  }
}
