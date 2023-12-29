package io.memoria.reactive.kafka;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.stream.EventRepo;
import io.memoria.reactive.eventsourcing.stream.EventRoute;
import io.vavr.collection.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;

public class KafkaEventRepo implements EventRepo {
  private final KafkaSender<String, String> producer;
  private final Map<String, Object> consumerConfig;
  private final EventRoute route;
  private final Duration lastEventTimeout;
  private final TextTransformer transformer;

  public KafkaEventRepo(Map<String, Object> producerConfig,
                        Map<String, Object> consumerConfig,
                        EventRoute route,
                        Duration lastEventTimeout,
                        TextTransformer transformer) {
    this.consumerConfig = consumerConfig;
    this.route = route;
    this.lastEventTimeout = lastEventTimeout;
    this.transformer = transformer;
    var senderOptions = SenderOptions.<String, String>create(producerConfig.toJavaMap());
    this.producer = KafkaSender.create(senderOptions);
  }

  @Override
  public Mono<Event> pub(Event event) {
    return producer.send(Mono.fromCallable(() -> toRecord(event))).map(SenderResult::correlationMetadata).single();
  }

  @Override
  public Flux<Event> sub() {
    var receiver = KafkaReceiver.create(KafkaUtils.receiveOptions(route.topic(), route.partition(), consumerConfig));
    return receiver.receive().concatMap(this::toEvent);
  }

  @Override
  public Mono<Event> last() {
    return Mono.fromCallable(() -> KafkaUtils.lastKey(route.topic(),
                                                      route.partition(),
                                                      lastEventTimeout,
                                                      consumerConfig))
               .flatMap(ReactorUtils::optionToMono)
               .flatMap(this::toEvent);
  }

  private SenderRecord<String, String, Event> toRecord(Event event) {
    var partition = event.partition(route.totalPartitions());
    var key = event.meta().eventId().value();
    var payload = transformer.serialize(event).get();
    return SenderRecord.create(route.topic(), partition, null, key, payload, event);
  }

  private Mono<Event> toEvent(ConsumerRecord<String, String> record) {
    return ReactorUtils.tryToMono(() -> transformer.deserialize(record.value(), Event.class));
  }
}
