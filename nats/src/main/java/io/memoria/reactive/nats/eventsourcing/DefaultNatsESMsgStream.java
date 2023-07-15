package io.memoria.reactive.nats.eventsourcing;

import io.memoria.reactive.core.stream.ESMsg;
import io.nats.client.Connection;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.api.StreamInfo;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DefaultNatsESMsgStream implements NatsESMsgStream {
  private static final Logger log = LoggerFactory.getLogger(DefaultNatsESMsgStream.class.getName());
  private final NatsConfig natsConfig;
  private final Connection nc;

  DefaultNatsESMsgStream(Connection nc, NatsConfig natsConfig) {
    this.natsConfig = natsConfig;
    this.nc = nc;
    this.natsConfig.configs()
                   .map(NatsUtils::toStreamConfiguration)
                   .map(c -> NatsUtils.createOrUpdateStream(nc, c))
                   .map(Try::get)
                   .map(StreamInfo::toString)
                   .forEach(log::info);
  }

  @Override
  public Mono<String> last(String topic, int partition) {
    var topicConfig = this.natsConfig.find(topic, partition).get();
    return Mono.fromCallable(() -> NatsUtils.jetStreamSubLast(nc, topicConfig))
               .flatMapMany(sub -> this.fetchBatch(sub, topicConfig))
               .next()
               .map(NatsUtils::toMsg)
               .map(ESMsg::key);
  }

  @Override
  public Mono<ESMsg> pub(ESMsg msg) {
    return Mono.fromCallable(() -> NatsUtils.publishMsg(nc, msg)).flatMap(Mono::fromFuture).thenReturn(msg);
  }

  @Override
  public Flux<ESMsg> sub(String topic, int partition) {
    var topicConfig = this.natsConfig.find(topic, partition).get();
    return Mono.fromCallable(() -> NatsUtils.jetStreamSub(nc, topicConfig))
               .flatMapMany(sub -> this.fetchBatch(sub, topicConfig).repeat())
               .map(NatsUtils::toMsg);
  }

  private Flux<Message> fetchBatch(JetStreamSubscription sub, TopicConfig config) {
    return Mono.fromCallable(() -> sub.fetch(config.fetchBatchSize(), config.fetchMaxWait()))
               .flatMapMany(Flux::fromIterable)
               .doOnNext(Message::ack);
  }
}
