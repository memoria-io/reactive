package io.memoria.reactive.nats.msg.stream;

import io.memoria.reactive.core.msg.stream.Msg;
import io.memoria.reactive.core.msg.stream.MsgStream;
import io.memoria.reactive.nats.NatsConfig;
import io.memoria.reactive.nats.NatsUtils;
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

  public NatsMsgStream(NatsConfig natsConfig, Scheduler scheduler) {
    this.natsConfig = natsConfig;
    this.scheduler = scheduler;
  }

  @Override
  public Mono<Msg> pub(String topic, int partition, Msg msg) {
    try (var nc = NatsUtils.createConnection(this.natsConfig)) {
      var opts = PublishOptions.builder().clearExpected().messageId(msg.key()).build();
      var js = nc.jetStream();
      return Mono.fromCallable(() -> natsMessage(topic, partition, msg))
                 .map(message -> js.publishAsync(message, opts))
                 .flatMap(Mono::fromFuture)
                 .map(ack -> msg);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    try (var nc = NatsUtils.createConnection(this.natsConfig)) {
      var js = nc.jetStream();
      return NatsUtils.fetchAllMessages(js, natsConfig, topic, partition).map(this::toESMsg).subscribeOn(scheduler);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    try (var nc = NatsUtils.createConnection(this.natsConfig)) {
      var js = nc.jetStream();
      var sub = NatsUtils.createSubscription(js, DeliverPolicy.Last, topic, partition);
      return NatsUtils.fetchLastMessage(sub, natsConfig).map(this::toESMsg).subscribeOn(scheduler);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static NatsMessage natsMessage(String topic, int partition, Msg msg) {
    var subjectName = NatsUtils.subjectName(topic, partition);
    var headers = new Headers();
    headers.add(ID_HEADER, msg.key());
    return NatsMessage.builder().subject(subjectName).headers(headers).data(msg.value()).build();
  }

  Msg toESMsg(Message message) {
    String key = message.getHeaders().getFirst(NatsUtils.ID_HEADER);
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return new Msg(key, value);
  }
}
