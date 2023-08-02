package io.memoria.reactive.nats;

import io.memoria.reactive.core.msg.stream.Msg;
import io.memoria.reactive.core.msg.stream.MsgStream;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.memoria.reactive.nats.NatsUtils.ID_HEADER;

public class NatsMsgStream implements MsgStream {
  private static final Logger log = LoggerFactory.getLogger(NatsMsgStream.class.getName());
  private final NatsConfig natsConfig;
  private final Scheduler scheduler;
  private final Connection connection;
  private final JetStream jetStream;

  public NatsMsgStream(NatsConfig natsConfig, Scheduler scheduler) throws IOException, InterruptedException {
    this.natsConfig = natsConfig;
    this.scheduler = scheduler;
    this.connection = NatsUtils.createConnection(this.natsConfig);
    this.jetStream = connection.jetStream();
  }

  @Override
  public Mono<Msg> pub(String topic, int partition, Msg msg) {
    var opts = PublishOptions.builder().clearExpected().messageId(msg.key()).build();
    return Mono.fromCallable(() -> natsMessage(topic, partition, msg))
               .map(message -> jetStream.publishAsync(message, opts))
               .flatMap(Mono::fromFuture)
               .map(ack -> msg);
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    return NatsUtils.fetchAllMessages(jetStream, natsConfig, topic, partition)
                    .doOnNext(Message::ack)
                    .map(NatsMsgStream::toESMsg)
                    .subscribeOn(scheduler);
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    var sub = NatsUtils.createSubscription(jetStream, DeliverPolicy.Last, topic, partition);
    return NatsUtils.fetchLastMessage(sub, natsConfig).map(NatsMsgStream::toESMsg).subscribeOn(scheduler);
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  static NatsMessage natsMessage(String topic, int partition, Msg msg) {
    var subjectName = NatsUtils.subjectName(topic, partition);
    var headers = new Headers();
    headers.add(ID_HEADER, msg.key());
    return NatsMessage.builder().subject(subjectName).headers(headers).data(msg.value()).build();
  }

  static Msg toESMsg(Message message) {
    String key = message.getHeaders().getFirst(NatsUtils.ID_HEADER);
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return new Msg(key, value);
  }
}
