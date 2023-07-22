package io.memoria.reactive.nats.eventsourcing;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.nats.TopicConfig;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
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

public class NatsCommandStream<C extends Command> implements CommandStream<C> {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandStream.class.getName());
  private final NatsConfig natsConfig;
  private final Connection nc;
  private final JetStream js;
  private final Class<C> cClass;
  private final TextTransformer transformer;

  public NatsCommandStream(NatsConfig natsConfig, Class<C> cClass, TextTransformer transformer)
          throws IOException, InterruptedException {
    this(natsConfig, NatsUtils.natsConnection(natsConfig), cClass, transformer);
  }

  public NatsCommandStream(NatsConfig natsConfig, Connection connection, Class<C> cClass, TextTransformer transformer)
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
  public Mono<C> pub(String topic, int partition, C cmd) {
    var opts = PublishOptions.builder().clearExpected().messageId(cmd.commandId().value()).build();
    return ReactorUtils.tryToMono(() -> transformer.serialize(cmd))
                       .map(cmdValue -> toMessage(topic, partition, cmd, cmdValue))
                       .map(message -> js.publishAsync(message, opts))
                       .flatMap(Mono::fromFuture)
                       .thenReturn(cmd);
  }

  @Override
  public Flux<C> sub(String topic, int partition) {
    var topicConfig = this.natsConfig.find(topic, partition).get();
    return Mono.fromCallable(() -> NatsUtils.jetStreamSub(js, topicConfig, DeliverPolicy.All))
               .flatMapMany(sub -> this.fetchBatch(sub, topicConfig).repeat())
               .concatMap(this::toMsg);
  }

  Flux<Message> fetchBatch(JetStreamSubscription sub, TopicConfig topicConfig) {
    return Mono.fromCallable(() -> sub.fetch(topicConfig.fetchBatchSize(), topicConfig.fetchMaxWait()))
               .flatMapMany(Flux::fromIterable)
               .doOnNext(Message::ack);
  }

  Message toMessage(String topic, int partition, C cmd, String cmdValue) {
    var subjectName = TopicConfig.subjectName(topic, partition);
    var headers = new Headers();
    headers.add(NatsUtils.ID_HEADER, cmd.commandId().value());
    return NatsMessage.builder().subject(subjectName).headers(headers).data(cmdValue).build();
  }

  Mono<C> toMsg(Message message) {
    return Mono.fromCallable(() -> new String(message.getData(), StandardCharsets.UTF_8))
               .flatMap(value -> ReactorUtils.tryToMono(() -> transformer.deserialize(value, cClass)));
  }
}
