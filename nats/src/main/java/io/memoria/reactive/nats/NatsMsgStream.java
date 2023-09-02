package io.memoria.reactive.nats;

import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.core.stream.MsgStream;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.PublishOptions;
import io.nats.client.api.DeliverPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;

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
    return Mono.fromCallable(() -> NatsUtils.natsMessage(topic, partition, msg))
               .map(message -> jetStream.publishAsync(message, opts))
               .flatMap(Mono::fromFuture)
               .map(ack -> msg);
  }

  @Override
  public Flux<Msg> sub(String topic, int partition) {
    return NatsUtils.fetchAllMessages(jetStream, natsConfig, topic, partition)
                    .map(NatsUtils::toESMsg)
                    .subscribeOn(scheduler);
  }

  @Override
  public Mono<Msg> last(String topic, int partition) {
    return ReactorUtils.tryToMono(() -> NatsUtils.createSubscription(jetStream, DeliverPolicy.Last, topic, partition))
                       .flatMap(sub -> NatsUtils.fetchLastMessage(sub, natsConfig)
                                                .map(NatsUtils::toESMsg)
                                                .subscribeOn(scheduler));
  }

  @Override
  public void close() throws Exception {
    log.info("Closing connection:{}", connection.getServerInfo());
    connection.close();
  }
}
