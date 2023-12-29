package io.memoria.reactive.nats;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.stream.EventRepo;
import io.memoria.reactive.eventsourcing.stream.EventRoute;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;

import static io.memoria.reactive.nats.NatsUtils.defaultConsumerConfigs;
import static io.memoria.reactive.nats.NatsUtils.toPartitionedSubjectName;
import static io.memoria.reactive.nats.NatsUtils.toSubscriptionName;

public class NatsEventRepo implements EventRepo {
  private static final Logger log = LoggerFactory.getLogger(NatsEventRepo.class.getName());
  private final JetStream jetStream;
  private final PullSubscribeOptions subscribeOptions;
  private final EventRoute route;

  // Polling Config
  private final int fetchBatchSize;
  private final Duration fetchMaxWait;

  // SerDes
  private final TextTransformer transformer;

  /**
   * Constructor with default settings
   */
  public NatsEventRepo(Connection connection, EventRoute route, TextTransformer transformer) throws IOException {
    this(connection,
         defaultConsumerConfigs(toSubscriptionName(route.topic(), route.partition())).build(),
         route,
         100,
         Duration.ofMillis(100),
         transformer);
  }

  public NatsEventRepo(Connection connection,
                       ConsumerConfiguration consumerConfig,
                       EventRoute route,
                       int fetchBatchSize,
                       Duration fetchMaxWait,
                       TextTransformer transformer) throws IOException {
    this.jetStream = connection.jetStream();
    this.route = route;
    this.subscribeOptions = PullSubscribeOptions.builder().configuration(consumerConfig).build();
    this.fetchBatchSize = fetchBatchSize;
    this.fetchMaxWait = fetchMaxWait;
    this.transformer = transformer;
  }

  @Override
  public Mono<Event> pub(Event event) {
    var cmdId = event.meta().eventId().value();
    var opts = PublishOptions.builder().clearExpected().messageId(cmdId).build();
    return Mono.fromCallable(() -> toNatsMessage(event))
               .flatMap(nm -> Mono.fromFuture(jetStream.publishAsync(nm, opts)))
               .map(_ -> event);
  }

  @Override
  public Flux<Event> sub() {
    var subject = toPartitionedSubjectName(route.topic(), route.partition());
    return Mono.fromCallable(() -> jetStream.subscribe(subject, subscribeOptions))
               .flatMapMany(sub -> NatsUtils.fetchMessages(sub, fetchBatchSize, fetchMaxWait))
               .concatMap(this::toEvent);
  }

  @Override
  public Mono<Event> last() {
    var subject = toPartitionedSubjectName(route.topic(), route.partition());
    return Mono.fromCallable(() -> jetStream.subscribe(subject, subscribeOptions))
               .flatMap(this::fetchLastMessage)
               .flatMap(this::toEvent);
  }

  private NatsMessage toNatsMessage(Event event) {
    var partition = event.partition(route.totalPartitions());
    var subject = toPartitionedSubjectName(route.topic(), partition);
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
