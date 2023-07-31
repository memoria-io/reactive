package io.memoria.reactive.nats.eventsourcing.stream;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class NatsCommandStream<C extends Command> implements CommandStream<C> {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandStream.class.getName());
  private final NatsConfig natsConfig;
  private final JetStream js;
  private final Class<C> cClass;
  private final TextTransformer transformer;
  private final Scheduler scheduler;

  public NatsCommandStream(NatsConfig natsConfig, Class<C> cClass, TextTransformer transformer, Scheduler scheduler)
          throws IOException, InterruptedException {
    this(natsConfig, NatsUtils.natsConnection(natsConfig), cClass, transformer, scheduler);
  }

  public NatsCommandStream(NatsConfig natsConfig,
                           Connection connection,
                           Class<C> cClass,
                           TextTransformer transformer,
                           Scheduler scheduler) throws IOException {
    this.natsConfig = natsConfig;
    this.js = connection.jetStream();
    this.cClass = cClass;
    this.transformer = transformer;
    this.scheduler = scheduler;
  }

  @Override
  public Mono<C> pub(String topic, int partition, C cmd) {
    var opts = PublishOptions.builder().clearExpected().messageId(cmd.commandId().value()).build();
    return ReactorUtils.tryToMono(() -> transformer.serialize(cmd))
                       .map(cmdValue -> toMessage(topic, partition, cmd, cmdValue))
                       .map(message -> js.publishAsync(message, opts))
                       .flatMap(Mono::fromFuture)
                       .map(ack -> cmd);
  }

  @Override
  public Flux<C> sub(String topic, int partition) {
    return NatsUtils.fetchAllMessages(js, natsConfig, topic, partition)
                    .concatMap(this::toCommand)
                    .subscribeOn(scheduler);
  }

  Message toMessage(String topic, int partition, C cmd, String cmdValue) {
    var subjectName = NatsUtils.subjectName(topic, partition);
    var headers = new Headers();
    headers.add(NatsUtils.ID_HEADER, cmd.commandId().value());
    return NatsMessage.builder().subject(subjectName).headers(headers).data(cmdValue).build();
  }

  Mono<C> toCommand(Message message) {
    return Mono.fromCallable(() -> new String(message.getData(), StandardCharsets.UTF_8))
               .flatMap(value -> ReactorUtils.tryToMono(() -> transformer.deserialize(value, cClass)));
  }
}
