package io.memoria.reactive.nats.messaging.stream;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.messaging.stream.ESMsg;
import io.memoria.reactive.core.messaging.stream.ESMsgStream;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.NatsMessage;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class NatsESMsgStream implements ESMsgStream {
  private static final Logger log = LoggerFactory.getLogger(NatsESMsgStream.class.getName());
  private final NatsConfig natsConfig;
  private final JetStream js;
  private final TextTransformer transformer;
  private final Scheduler scheduler;

  public NatsESMsgStream(NatsConfig natsConfig, Scheduler scheduler, TextTransformer transformer)
          throws IOException, InterruptedException {
    this(natsConfig, NatsUtils.natsConnection(natsConfig), transformer, scheduler);
  }

  public NatsESMsgStream(NatsConfig natsConfig, Connection connection, TextTransformer transformer, Scheduler scheduler)
          throws IOException {
    this.natsConfig = natsConfig;
    this.js = connection.jetStream();
    this.transformer = transformer;
    this.scheduler = scheduler;
  }

  @Override
  public Mono<ESMsg> pub(String topic, int partition, ESMsg msg) {
    return ReactorUtils.tryToMono(() -> transformer.serialize(msg))
                       .map(messageValue -> natsMessage(topic, partition, messageValue))
                       .map(message -> publishESMsg(msg.key(), message))
                       .flatMap(Mono::fromFuture)
                       .map(ack -> msg);
  }

  @Override
  public Flux<ESMsg> sub(String topic, int partition) {
    return NatsUtils.fetchAllMessages(js, natsConfig, topic, partition)
                    .map(this::toESMsg)
                    .flatMap(tr -> ReactorUtils.tryToMono(() -> tr))
                    .subscribeOn(scheduler);
  }

  @Override
  public Mono<String> last(String topic, int partition) {
    return NatsUtils.fetchLastMessage(js, natsConfig, topic, partition)
                    .map(this::toESMsg)
                    .flatMap(tr -> ReactorUtils.tryToMono(() -> tr))
                    .map(ESMsg::key)
                    .subscribeOn(scheduler);
  }

  static NatsMessage natsMessage(String topic, int partition, String value) {
    var subjectName = NatsUtils.subjectName(topic, partition);
    return NatsMessage.builder().subject(subjectName).data(value).build();
  }

  CompletableFuture<PublishAck> publishESMsg(String key, Message message) {
    var opts = PublishOptions.builder().clearExpected().messageId(key).build();
    return js.publishAsync(message, opts);
  }

  Try<ESMsg> toESMsg(Message message) {
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return this.transformer.deserialize(value, ESMsg.class);
  }
}
