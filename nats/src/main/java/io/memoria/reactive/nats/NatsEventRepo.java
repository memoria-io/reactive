package io.memoria.reactive.nats;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.stream.EventRepo;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.NatsMessage;
import io.vavr.collection.List;
import io.vavr.collection.Traversable;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Function;

import static io.memoria.reactive.nats.NatsUtils.toPartitionedSubjectName;
import static io.memoria.reactive.nats.NatsUtils.toSubscriptionName;

public class NatsEventRepo implements EventRepo {
  private static final Logger log = LoggerFactory.getLogger(NatsEventRepo.class.getName());
  private final JetStream jetStream;
  private final PullSubscribeOptions subscribeOptions;
  private final String topic;
  private final String subjectName;
  private final int totalPartitions;

  // Polling Config
  private final Duration pollTimeout;
  private final int fetchBatchSize;
  private final Duration fetchMaxWait;

  // SerDes
  private final TextTransformer transformer;

  /**
   * Constructor with default settings
   */
  public NatsEventRepo(Connection connection, String topic, int totalPartitions, TextTransformer transformer)
          throws IOException {
    this(connection,
         NatsUtils.defaultCommandConsumerConfigs(toSubscriptionName(topic)).build(),
         topic,
         totalPartitions,
         Duration.ofMillis(1000),
         100,
         Duration.ofMillis(100),
         transformer);
  }

  public NatsEventRepo(Connection connection,
                       ConsumerConfiguration consumerConfig,
                       String topic,
                       int totalPartitions,
                       Duration pollTimeout,
                       int fetchBatchSize,
                       Duration fetchMaxWait,
                       TextTransformer transformer) throws IOException {
    this.jetStream = connection.jetStream();
    this.subscribeOptions = PullSubscribeOptions.builder().stream(topic).configuration(consumerConfig).build();
    this.topic = topic;
    this.subjectName = toPartitionedSubjectName(topic);
    this.totalPartitions = totalPartitions;
    this.pollTimeout = pollTimeout;
    this.fetchBatchSize = fetchBatchSize;
    this.fetchMaxWait = fetchMaxWait;
    this.transformer = transformer;
  }

  @Override
  public Mono<Event> publish(Event event) {
    var cmdId = event.meta().eventId().value();
    var opts = PublishOptions.builder().clearExpected().messageId(cmdId).build();
    return Mono.fromCallable(() -> toNatsMessage(event))
               .flatMap(nm -> Mono.fromFuture(jetStream.publishAsync(nm, opts)))
               .map(_ -> event);
  }

  @Override
  public Flux<Event> subscribe(int partition) {
    return Mono.fromCallable(() -> jetStream.subscribe(subjectName, subscribeOptions)).flatMapMany(this::fetchMessages);
  }

  @Override
  public Mono<Event> last(int partition) {
    return Mono.fromCallable(() -> jetStream.subscribe(subjectName, subscribeOptions))
               .flatMap(this::fetchLastMessage)
               .flatMap(this::toEvent);
  }

  private Flux<Event> fetchMessages(JetStreamSubscription sub) {
    return Flux.generate((SynchronousSink<Flux<Message>> sink) -> {
      var tr = Try.of(() -> sub.fetch(fetchBatchSize, fetchMaxWait));
      if (tr.isSuccess()) {
        List<Message> messages = List.ofAll(tr.get()).dropWhile(Message::isStatusMessage);
        sink.next(Flux.fromIterable(messages));
      } else {
        sink.error(tr.getCause());
      }
    }).concatMap(Function.identity()).concatMap(this::toEvent);
  }

  private NatsMessage toNatsMessage(Event event) {
    var partition = event.partition(totalPartitions);
    var subject = toPartitionedSubjectName(topic, partition);
    var payload = transformer.serialize(event).get();
    return NatsMessage.builder().subject(subject).data(payload).build();
  }

  private Mono<Event> toEvent(Message message) {
    var payload = new String(message.getData());
    return ReactorUtils.tryToMono(() -> transformer.deserialize(payload, Event.class));

  }

  public Mono<Message> fetchLastMessage(JetStreamSubscription sub) {
    return Mono.fromCallable(() -> sub.fetch(fetchBatchSize, fetchMaxWait))
               .map(List::ofAll)
               .map(Traversable::lastOption)
               .flatMap(ReactorUtils::optionToMono);
  }
}
