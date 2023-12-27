package io.memoria.reactive.nats;

import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.stream.CommandRepo;
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

import static io.memoria.reactive.nats.NatsUtils.toPartitionedSubjectName;
import static io.memoria.reactive.nats.NatsUtils.toSubscriptionName;

public class NatsCommandRepo implements CommandRepo {
  private static final Logger log = LoggerFactory.getLogger(NatsCommandRepo.class.getName());
  private final JetStream jetStream;
  private final PullSubscribeOptions subscribeOptions;
  private final String topic;
  private final String subjectName;
  private final int totalPartitions;

  // Polling Config
  private final int fetchBatchSize;
  private final Duration fetchMaxWait;

  // SerDes
  private final TextTransformer transformer;

  /**
   * Constructor with default settings
   */
  public NatsCommandRepo(Connection connection, String topic, int totalPartitions, TextTransformer transformer)
          throws IOException {
    this(connection,
         NatsUtils.defaultCommandConsumerConfigs(toSubscriptionName(topic)).build(),
         topic,
         totalPartitions,
         100,
         Duration.ofMillis(100),
         transformer);
  }

  public NatsCommandRepo(Connection connection,
                         ConsumerConfiguration consumerConfig,
                         String topic,
                         int totalPartitions,
                         int fetchBatchSize,
                         Duration fetchMaxWait,
                         TextTransformer transformer) throws IOException {
    this.jetStream = connection.jetStream();
    this.subscribeOptions = PullSubscribeOptions.builder().stream(topic).configuration(consumerConfig).build();
    this.topic = topic;
    this.subjectName = toPartitionedSubjectName(topic);
    this.totalPartitions = totalPartitions;
    this.fetchBatchSize = fetchBatchSize;
    this.fetchMaxWait = fetchMaxWait;
    this.transformer = transformer;
  }

  @Override
  public Mono<Command> publish(Command command) {
    var cmdId = command.meta().commandId().value();
    var opts = PublishOptions.builder().clearExpected().messageId(cmdId).build();
    return Mono.fromCallable(() -> toNatsMessage(command))
               .flatMap(nm -> Mono.fromFuture(jetStream.publishAsync(nm, opts)))
               .map(_ -> command);
  }

  @Override
  public Flux<Command> subscribe() {
    return Mono.fromCallable(() -> jetStream.subscribe(subjectName, subscribeOptions))
               .flatMapMany(sub -> NatsUtils.fetchMessages(sub, fetchBatchSize, fetchMaxWait))
               .concatMap(this::toCommand);
  }

  private NatsMessage toNatsMessage(Command command) {
    var partition = command.partition(totalPartitions);
    var subject = toPartitionedSubjectName(topic, partition);
    var payload = transformer.serialize(command).get();
    return NatsMessage.builder().subject(subject).data(payload).build();
  }

  private Mono<Command> toCommand(Message message) {
    var payload = new String(message.getData());
    return ReactorUtils.tryToMono(() -> transformer.deserialize(payload, Command.class));

  }
  //  @Override
  //  public Mono<Msg> last(String topic, int partition) {
  //    return ReactorUtils.tryToMono(() -> NatsUtils.createSubscription(jetStream, DeliverPolicy.Last, topic, partition))
  //                       .flatMap(sub -> NatsUtils.fetchLastMessage(sub, natsConfig)
  //                                                .map(NatsUtils::toESMsg)
  //                                                .subscribeOn(scheduler));
  //  }
  //  public static Mono<Message> fetchLastMessage(JetStreamSubscription sub, NatsConfig config) {
  //    return Mono.fromCallable(() -> sub.fetch(config.fetchBatchSize(), config.fetchMaxWait()))
  //               .map(List::ofAll)
  //               .map(Traversable::lastOption)
  //               .flatMap(ReactorUtils::optionToMono);
  //  }

}
