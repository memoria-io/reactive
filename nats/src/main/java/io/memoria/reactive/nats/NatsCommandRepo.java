package io.memoria.reactive.nats;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.stream.CommandRepo;
import io.memoria.reactive.eventsourcing.stream.CommandRoute;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.NatsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;

import static io.memoria.reactive.nats.NatsUtils.defaultConsumerConfigs;
import static io.memoria.reactive.nats.NatsUtils.toPartitionedSubjectName;
import static io.memoria.reactive.nats.NatsUtils.toSubscriptionName;

public class NatsCommandRepo implements CommandRepo {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandRepo.class.getName());
  private final JetStream jetStream;
  private final PullSubscribeOptions subscribeOptions;
  private final CommandRoute eventRoute;

  // Polling Config
  private final int fetchBatchSize;
  private final Duration fetchMaxWait;

  // SerDes
  private final TextTransformer transformer;

  /**
   * Constructor with default settings
   */
  public NatsCommandRepo(Connection connection, CommandRoute route, TextTransformer transformer) throws IOException {
    this(connection,
         defaultConsumerConfigs(toSubscriptionName(route.topic(), route.partition())).build(),
         route,
         100,
         Duration.ofMillis(100),
         transformer);
  }

  public NatsCommandRepo(Connection connection,
                         ConsumerConfiguration consumerConfig,
                         CommandRoute eventRoute,
                         int fetchBatchSize,
                         Duration fetchMaxWait,
                         TextTransformer transformer) throws IOException {
    this.jetStream = connection.jetStream();
    this.subscribeOptions = PullSubscribeOptions.builder().configuration(consumerConfig).build();
    this.eventRoute = eventRoute;
    this.fetchBatchSize = fetchBatchSize;
    this.fetchMaxWait = fetchMaxWait;
    this.transformer = transformer;
  }

  @Override
  public Mono<Command> pub(Command command) {
    var cmdId = command.meta().commandId().value();
    var opts = PublishOptions.builder().clearExpected().messageId(cmdId).build();
    return Mono.fromCallable(() -> toNatsMessage(command))
               .flatMap(nm -> Mono.fromFuture(jetStream.publishAsync(nm, opts)))
               .map(_ -> command);
  }

  @Override
  public Flux<Command> sub() {
    var subjectName = toPartitionedSubjectName(eventRoute.topic(), eventRoute.partition());
    return Mono.fromCallable(() -> jetStream.subscribe(subjectName, subscribeOptions))
               .flatMapMany(sub -> NatsUtils.fetchMessages(sub, fetchBatchSize, fetchMaxWait))
               .concatMap(this::toCommand);
  }

  private NatsMessage toNatsMessage(Command command) {
    var partition = command.partition(eventRoute.totalPartitions());
    var subject = toPartitionedSubjectName(eventRoute.topic(), partition);
    var payload = transformer.serialize(command).get();
    return NatsMessage.builder().subject(subject).data(payload).build();
  }

  private Mono<Command> toCommand(Message message) {
    var payload = new String(message.getData());
    return ReactorUtils.tryToMono(() -> transformer.deserialize(payload, Command.class));

  }
}
