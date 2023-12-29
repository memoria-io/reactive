package io.memoria.reactive.kafka;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.stream.CommandRepo;
import io.memoria.reactive.eventsourcing.stream.CommandRoute;
import io.vavr.collection.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

public class KafkaCommandRepo implements CommandRepo {
  private final KafkaSender<String, String> producer;
  private final Map<String, Object> consumerConfig;
  private final CommandRoute route;
  private final TextTransformer transformer;

  public KafkaCommandRepo(Map<String, Object> producerConfig,
                          Map<String, Object> consumerConfig,
                          CommandRoute route,
                          TextTransformer transformer) {
    this.consumerConfig = consumerConfig;
    this.route = route;
    this.transformer = transformer;
    var senderOptions = SenderOptions.<String, String>create(producerConfig.toJavaMap());
    this.producer = KafkaSender.create(senderOptions);
  }

  @Override
  public Mono<Command> pub(Command command) {
    return producer.send(Mono.fromCallable(() -> toRecord(command))).map(SenderResult::correlationMetadata).single();
  }

  @Override
  public Flux<Command> sub() {
    var receiver = KafkaReceiver.create(KafkaUtils.receiveOptions(route.topic(), route.partition(), consumerConfig));
    return receiver.receive().concatMap(this::toCommand);
  }

  private SenderRecord<String, String, Command> toRecord(Command command) {
    var partition = command.partition(route.totalPartitions());
    var key = command.meta().commandId().value();
    var payload = transformer.serialize(command).get();
    return SenderRecord.create(route.topic(), partition, null, key, payload, command);
  }

  private Mono<Command> toCommand(ReceiverRecord<String, String> record) {
    return ReactorUtils.tryToMono(() -> transformer.deserialize(record.value(), Command.class));
  }
}
