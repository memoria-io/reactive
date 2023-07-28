package io.memoria.reactive.nats.messaging.stream;

import io.memoria.reactive.core.messaging.stream.ESMsg;
import io.memoria.reactive.core.messaging.stream.ESMsgStream;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

public class NatsESMsgStream implements ESMsgStream {
  private static final Logger log = LoggerFactory.getLogger(NatsESMsgStream.class.getName());
  private final NatsConfig natsConfig;
  private final JetStream js;
  private final Scheduler scheduler;

  public NatsESMsgStream(NatsConfig natsConfig, Scheduler scheduler) throws IOException, InterruptedException {
    this(natsConfig, NatsUtils.natsConnection(natsConfig), scheduler);
  }

  public NatsESMsgStream(NatsConfig natsConfig, Connection connection, Scheduler scheduler) throws IOException {
    this.natsConfig = natsConfig;
    this.js = connection.jetStream();
    this.scheduler = scheduler;
  }

  @Override
  public Mono<ESMsg> pub(ESMsg msg) {
    return Mono.fromFuture(() -> publishESMsg(msg)).map(ack -> msg);
  }

  @Override
  public Flux<ESMsg> sub(String topic, int partition) {
    return Mono.fromCallable(() -> NatsUtils.jetStreamSub(js, DeliverPolicy.All, topic, partition))
               .flatMapMany(sub -> NatsUtils.fetch(sub, natsConfig))
               .subscribeOn(scheduler)
               .map(NatsUtils::acknowledge)
               .map(NatsESMsgStream::toESMsg);
  }

  @Override
  public Mono<String> last(String topic, int partition) {
    return Mono.fromCallable(() -> NatsUtils.jetStreamSub(js, DeliverPolicy.Last, topic, partition))
               .map(sub -> NatsUtils.blockingFetchLast(sub, natsConfig))
               .subscribeOn(Schedulers.boundedElastic())
               .flatMap(ReactorUtils::optionToMono)
               .map(NatsESMsgStream::toESMsg)
               .map(ESMsg::key);
  }

  public CompletableFuture<PublishAck> publishESMsg(ESMsg msg) {
    var message = toMessage(msg);
    var opts = PublishOptions.builder().clearExpected().messageId(msg.key()).build();
    return js.publishAsync(message, opts);
  }

  public static Message toMessage(ESMsg msg) {
    var subjectName = NatsUtils.subjectName(msg.topic(), msg.partition());
    var headers = new Headers();
    headers.add(NatsUtils.ID_HEADER, msg.key());
    return NatsMessage.builder().subject(subjectName).headers(headers).data(msg.value()).build();
  }

  public static ESMsg toESMsg(Message message) {
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    var tp = NatsUtils.topicPartition(message.getSubject());
    String key = message.getHeaders().getFirst(NatsUtils.ID_HEADER);
    return new ESMsg(tp._1, tp._2, key, value);
  }
}
