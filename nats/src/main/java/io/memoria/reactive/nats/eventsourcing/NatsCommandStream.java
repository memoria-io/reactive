package io.memoria.reactive.nats.eventsourcing;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.memoria.reactive.nats.TopicConfig;
import io.nats.client.Connection;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
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
import java.util.concurrent.CompletableFuture;

public class NatsCommandStream<C extends Command> implements CommandStream<C> {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandStream.class.getName());
  private final NatsConfig natsConfig;
  private final Connection nc;
  private final Class<C> cClass;
  private final TextTransformer transformer;

  public NatsCommandStream(NatsConfig natsConfig, Class<C> cClass, TextTransformer transformer)
          throws IOException, InterruptedException {
    this.natsConfig = natsConfig;
    this.nc = Nats.connect(NatsUtils.toOptions(natsConfig));
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
    return ReactorUtils.tryToMono(() -> transformer.serialize(cmd))
                       .flatMap(cmdValue -> publish(topic, partition, cmd, cmdValue))
                       .thenReturn(cmd);
  }

  @Override
  public Flux<C> sub(String topic, int partition) {
    var topicConfig = this.natsConfig.find(topic, partition).get();
    return Mono.fromCallable(() -> NatsUtils.jetStreamSub(nc, topicConfig))
               .flatMapMany(sub -> this.fetchBatch(sub, topicConfig).repeat())
               .concatMap(this::toMsg);
  }

  Mono<PublishAck> publish(String topic, int partition, C cmd, String cmdValue) {
    return Mono.fromCallable(() -> this.publishMsg(topic, partition, cmd, cmdValue)).flatMap(Mono::fromFuture);
  }

  Flux<Message> fetchBatch(JetStreamSubscription sub, TopicConfig config) {
    return Mono.fromCallable(() -> sub.fetch(config.fetchBatchSize(), config.fetchMaxWait()))
               .flatMapMany(Flux::fromIterable)
               .doOnNext(Message::ack);
  }

  CompletableFuture<PublishAck> publishMsg(String topic, int partition, C msg, String cmdValue) throws IOException {
    var message = toMessage(topic, partition, msg, cmdValue);
    var opts = PublishOptions.builder().clearExpected().messageId(msg.commandId().value()).build();
    return nc.jetStream().publishAsync(message, opts);
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
