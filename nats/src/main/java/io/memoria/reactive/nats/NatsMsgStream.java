package io.memoria.reactive.nats;

import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.stream.Msg;
import io.memoria.reactive.eventsourcing.stream.MsgStream;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Function;

import static io.memoria.reactive.nats.NatsUtils.defaultConsumerConfigs;
import static io.memoria.reactive.nats.NatsUtils.toPartitionedSubjectName;
import static io.memoria.reactive.nats.NatsUtils.toSubscriptionName;

public class NatsMsgStream implements MsgStream {
  private static final Logger log = LoggerFactory.getLogger(NatsMsgStream.class.getName());
  public static final String ID_HEADER = "ID_HEADER";
  private final JetStream jetStream;
  // Polling Config
  private final int fetchBatchSize;
  private final Duration fetchMaxWait;

  /**
   * Constructor with default settings
   */
  public NatsMsgStream(Connection connection) throws IOException {
    this(connection, 100, Duration.ofMillis(100));
  }

  public NatsMsgStream(Connection connection, int fetchBatchSize, Duration fetchMaxWait) throws IOException {
    this.jetStream = connection.jetStream();
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

    return Mono.fromCallable(() -> getSubscription(topic, partition))
               .flatMapMany(this::fetchMessages)
               .map(m -> toMsg(topic, partition, m))
               .subscribeOn(Schedulers.newSingle(Thread.ofVirtual().factory()));
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    return Mono.fromCallable(() -> fetchLastMessage(getSubscription(topic, partition)))
               .flatMap(ReactorUtils::optionToMono)
               .map(m -> toMsg(topic, partition, m));
  }

  public Flux<Message> fetchMessages(JetStreamSubscription sub) {
    return Flux.generate((SynchronousSink<Flux<Message>> sink) -> {
      var tr = Try.of(() -> sub.fetch(fetchBatchSize, fetchMaxWait));
      if (tr.isSuccess()) {
        sink.next(Flux.fromIterable(tr.get()));
      } else {
        sink.error(tr.getCause());
      }
    }).concatMap(Function.identity()).skipWhile(Message::isStatusMessage).doOnNext(Message::ack);
  }

  private Option<Message> fetchLastMessage(JetStreamSubscription sub) {
    return List.ofAll(sub.fetch(fetchBatchSize, fetchMaxWait)).lastOption();
  }

  private JetStreamSubscription getSubscription(String topic, int partition) throws JetStreamApiException, IOException {
    String name = toSubscriptionName(topic, partition);
    var consumerConfig = defaultConsumerConfigs(name).build();
    var pullOpts = PullSubscribeOptions.builder().name(name).configuration(consumerConfig).build();
    var subject = toPartitionedSubjectName(topic, partition);
    return jetStream.subscribe(subject, pullOpts);
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
}
