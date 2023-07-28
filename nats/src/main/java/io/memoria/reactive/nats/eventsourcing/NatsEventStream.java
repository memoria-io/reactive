package io.memoria.reactive.nats.eventsourcing;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.memoria.reactive.nats.NatsUtils.jetStreamSub;

public class NatsEventStream<E extends Event> implements EventStream<E> {
  private static final Logger log = LoggerFactory.getLogger(NatsEventStream.class.getName());
  private final NatsConfig config;
  private final JetStream js;
  private final Class<E> cClass;
  private final TextTransformer transformer;

  public NatsEventStream(NatsConfig config, Class<E> eventClass, TextTransformer transformer)
          throws IOException, InterruptedException {
    this(config, NatsUtils.natsConnection(config), eventClass, transformer);
  }

  public NatsEventStream(NatsConfig config, Connection connection, Class<E> cClass, TextTransformer transformer)
          throws IOException {
    this.config = config;
    this.js = connection.jetStream();
    this.cClass = cClass;
    this.transformer = transformer;
  }

  @Override
  public Mono<E> pub(String topic, int partition, E event) {
    return ReactorUtils.tryToMono(() -> transformer.serialize(event))
                       .map(value -> toMessage(topic, partition, event, value))
                       .map(js::publishAsync)
                       .flatMap(Mono::fromFuture)
                       .doOnNext(i -> System.out.println(i.getSeqno()))
                       .map(i -> event);
  }

  @Override
  public Flux<E> sub(String topic, int partition) {
    return Mono.fromCallable(() -> jetStreamSub(js, DeliverPolicy.All, topic, partition))
               .map(sub -> sub.fetch(config.fetchBatchSize(), config.fetchMaxWait()))
               .repeat()
               .concatMap(Flux::fromIterable)
               .concatMap(this::toEvent);
  }

  @Override
  public Mono<Id> last(String topic, int partition) {
    return Mono.fromCallable(() -> jetStreamSub(js, DeliverPolicy.Last, topic, partition))
               .map(sub -> sub.fetch(config.fetchBatchSize(), config.fetchMaxWait()))
               .flatMapMany(Flux::fromIterable)
               .singleOrEmpty()
               .flatMap(this::toEvent)
               .map(E::eventId);
  }

  Message toMessage(String topic, int partition, E event, String value) {
    var subjectName = NatsUtils.subjectName(topic, partition);
    var headers = new Headers();
    headers = headers.add(NatsUtils.ID_HEADER, event.eventId().value());
    //    return NatsMessage.builder().subject(subjectName).headers(headers).data(value).build();
    return NatsMessage.builder().subject(subjectName).data(value).build();
  }

  Mono<E> toEvent(Message message) {
    return Mono.fromCallable(() -> new String(message.getData(), StandardCharsets.UTF_8))
               .flatMap(value -> ReactorUtils.tryToMono(() -> transformer.deserialize(value, cClass)));
  }
}
