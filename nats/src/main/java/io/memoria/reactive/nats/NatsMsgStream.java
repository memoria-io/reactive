package io.memoria.reactive.nats;

import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.stream.Msg;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.vavr.collection.List;
import io.vavr.collection.Traversable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.memoria.reactive.nats.NatsUtils.defaultConsumerConfigs;
import static io.memoria.reactive.nats.NatsUtils.toPartitionedSubjectName;
import static io.memoria.reactive.nats.NatsUtils.toSubscriptionName;

public class NatsMsgStream implements MsgStream {
  private static final Logger log = LoggerFactory.getLogger(NatsMsgStream.class.getName());
  public static final String ID_HEADER = "ID_HEADER";
  private final JetStream jetStream;
  private final PullSubscribeOptions subscribeOptions;

  // Polling Config
  private final int fetchBatchSize;
  private final Duration fetchMaxWait;

  /**
   * Constructor with default settings
   */
  public NatsMsgStream(Connection connection, EventRoute route) throws IOException {
    this(connection,
         defaultConsumerConfigs(toSubscriptionName(route.topic(), route.partition())).build(),
         100,
         Duration.ofMillis(100));
  }

  public NatsMsgStream(Connection connection,
                       ConsumerConfiguration consumerConfig,
                       int fetchBatchSize,
                       Duration fetchMaxWait) throws IOException {
    this.jetStream = connection.jetStream();
    this.subscribeOptions = PullSubscribeOptions.builder().configuration(consumerConfig).build();
    this.fetchBatchSize = fetchBatchSize;
    this.fetchMaxWait = fetchMaxWait;
  }

  @Override
  public Mono<Msg> pub(Msg msg) {
    var opts = PublishOptions.builder().clearExpected().messageId(msg.key()).build();
    return Mono.fromCallable(() -> toNatsMessage(msg))
               .flatMap(nm -> Mono.fromFuture(jetStream.publishAsync(nm, opts)))
               .map(_ -> msg);
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    var subject = toPartitionedSubjectName(topic, partition);
    return Mono.fromCallable(() -> jetStream.subscribe(subject, subscribeOptions))
               .flatMapMany(sub -> NatsUtils.fetchMessages(sub, fetchBatchSize, fetchMaxWait))
               .map(m -> toMsg(topic, partition, m));
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    var subject = toPartitionedSubjectName(topic, partition);
    return Mono.fromCallable(() -> jetStream.subscribe(subject, subscribeOptions))
               .flatMap(this::fetchLastMessage)
               .map(m -> toMsg(topic, partition, m));
  }

  static NatsMessage toNatsMessage(Msg msg) {
    var subjectName = toPartitionedSubjectName(msg.topic(), msg.partition());
    var headers = new Headers();
    headers.add(ID_HEADER, msg.key());
    return NatsMessage.builder().subject(subjectName).headers(headers).data(msg.value()).build();
  }

  private Msg toMsg(String topic, int partition, Message message) {
    String key = message.getHeaders().getFirst(ID_HEADER);
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return new Msg(topic, partition, key, value);

  }

  public Mono<Message> fetchLastMessage(JetStreamSubscription sub) {
    return Mono.fromCallable(() -> sub.fetch(fetchBatchSize, fetchMaxWait))
               .map(List::ofAll)
               .map(Traversable::lastOption)
               .flatMap(ReactorUtils::optionToMono);
  }
}
