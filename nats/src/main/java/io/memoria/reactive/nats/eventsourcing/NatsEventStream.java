package io.memoria.reactive.nats.eventsourcing;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.nats.TopicConfig;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class NatsEventStream<E extends Event> implements EventStream<E> {
  private static final Logger log = LoggerFactory.getLogger(NatsEventStream.class.getName());
  private final NatsConfig natsConfig;
  private final Connection nc;
  private final JetStream js;
  private final Class<E> cClass;
  private final TextTransformer transformer;

  public NatsEventStream(NatsConfig natsConfig, Class<E> eventClass, TextTransformer transformer)
          throws IOException, InterruptedException {
    this(natsConfig, NatsUtils.natsConnection(natsConfig), eventClass, transformer);
  }

  public NatsEventStream(NatsConfig natsConfig, Connection connection, Class<E> cClass, TextTransformer transformer)
          throws IOException {
    this.natsConfig = natsConfig;
    this.nc = connection;
    this.js = nc.jetStream();
    this.cClass = cClass;
    this.transformer = transformer;
    this.natsConfig.configs()
                   .map(NatsUtils::toStreamConfiguration)
                   .map(c -> NatsUtils.createOrUpdateStream(nc, c))
                   .map(Try::get)
                   .map(StreamInfo::toString)
                   .forEach(log::info);
  }

  @Override
  public Mono<E> pub(String topic, int partition, E event) {
    //    System.out.println(event.eventId());
    //    var opts = PublishOptions.builder().clearExpected().messageId(event.eventId().value()).build();
    return ReactorUtils.tryToMono(() -> transformer.serialize(event))
                       .map(value -> toMessage(topic, partition, event, value))
                       .map(js::publishAsync)
                       .flatMap(Mono::fromFuture)
                       .doOnNext(i -> System.out.println(i.getSeqno()))
                       .map(i -> event);
  }

  @Override
  public Flux<E> sub(String topic, int partition) {
    var topicConfig = this.natsConfig.find(topic, partition).get();
    var sub = NatsUtils.jetStreamSub(js, topicConfig, DeliverPolicy.All);
    return Mono.fromCallable(() -> this.fetchBatch(sub, topicConfig))
               .repeat()
               .concatMap(Flux::fromIterable)
               .concatMap(this::toMsg);
  }

  @Override
  public Mono<Id> last(String topic, int partition) {
    var topicConfig = this.natsConfig.find(topic, partition).get();
    return Mono.fromCallable(() -> NatsUtils.jetStreamSub(js, topicConfig, DeliverPolicy.Last))
               .map(sub -> this.fetchBatch(sub, topicConfig))
               .flatMapMany(Flux::fromIterable)
               .singleOrEmpty()
               .flatMap(this::toMsg)
               .map(E::eventId);
  }

  List<Message> fetchBatch(JetStreamSubscription sub, TopicConfig config) {
    return sub.fetch(config.fetchBatchSize(), config.fetchMaxWait());
  }

  Message toMessage(String topic, int partition, E event, String value) {
    var subjectName = TopicConfig.subjectName(topic, partition);
    var headers = new Headers();
    headers = headers.add(NatsUtils.ID_HEADER, event.eventId().value());
    //    return NatsMessage.builder().subject(subjectName).headers(headers).data(value).build();
    return NatsMessage.builder().subject(subjectName).data(value).build();
  }

  Mono<E> toMsg(Message message) {
    return Mono.fromCallable(() -> new String(message.getData(), StandardCharsets.UTF_8))
               .flatMap(value -> ReactorUtils.tryToMono(() -> transformer.deserialize(value, cClass)));
  }
}
