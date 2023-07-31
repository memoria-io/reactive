package io.memoria.reactive.nats;

import io.memoria.reactive.core.reactor.ReactorUtils;
import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Traversable;
import io.vavr.control.Try;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Function;

/**
 * Utility class for directly using nats, this layer is for testing NATS java driver, and ideally it shouldn't have
 * Flux/Mono utilities, only pure java/nats APIs
 */
public class NatsUtils {
  public static final String ID_HEADER = "ID_HEADER";
  public static final String SPLIT_TOKEN = "_";
  public static final String SUBJECT_EXT = ".subject";

  private NatsUtils() {}

  public static String subjectName(String topic, int partition) {
    return streamName(topic, partition) + SUBJECT_EXT;
  }

  public static Tuple2<String, Integer> topicPartition(String subject) {
    var idx = subject.indexOf(SUBJECT_EXT);
    var s = subject.substring(0, idx).split(SPLIT_TOKEN);
    var topic = s[0];
    var partition = Integer.parseInt(s[1]);
    return Tuple.of(topic, partition);
  }

  public static Connection natsConnection(NatsConfig natsConfig) throws IOException, InterruptedException {
    return Nats.connect(Options.builder().server(natsConfig.url()).errorListener(errorListener()).build());
  }

  public static List<StreamInfo> createOrUpdateTopic(NatsConfig natsConfig, String topic, int numOfPartitions)
          throws IOException, InterruptedException, JetStreamApiException {
    var result = List.<StreamInfo>empty();
    try (var nc = NatsUtils.natsConnection(natsConfig)) {
      var streamConfigs = List.range(0, numOfPartitions)
                              .map(partition -> streamConfiguration(natsConfig, topic, partition));
      for (StreamConfiguration config : streamConfigs) {
        StreamInfo createOrUpdate = createOrUpdateStream(nc, config);
        result = result.append(createOrUpdate);
      }
    }
    return result;
  }

  public static Flux<Message> fetchAllMessagesForCmds(JetStream js,
                                                      NatsConfig natsConfig,
                                                      String topic,
                                                      int partition) {
    return Mono.fromCallable(() -> {
      System.out.printf("subscribing to %s %d%n", topic, partition);
      return jetStreamSub(js, DeliverPolicy.All, topic, partition);
    }).flatMapMany(sub -> fetchMessagesForCmds(sub, natsConfig.fetchBatchSize(), natsConfig.fetchMaxWait()));
  }

  public static Flux<Message> fetchAllMessages(JetStream js, NatsConfig natsConfig, String topic, int partition) {
    return Mono.fromCallable(() -> {
      System.out.printf("subscribing to %s %d%n", topic, partition);
      return jetStreamSub(js, DeliverPolicy.All, topic, partition);
    }).flatMapMany(sub -> fetchMessages(sub, natsConfig.fetchBatchSize(), natsConfig.fetchMaxWait()));
  }

  public static Mono<Message> fetchLastMessage(JetStream js, NatsConfig config, String topic, int partition) {
    return Mono.fromCallable(() -> jetStreamSub(js, DeliverPolicy.Last, topic, partition))
               .map(sub -> List.ofAll(sub.fetch(config.fetchBatchSize(), config.fetchMaxWait())))
               .map(Traversable::lastOption)
               .flatMap(ReactorUtils::optionToMono);
  }

  //  static Flux<List<Message>> fetchMessages(JetStreamSubscription sub, int fetchBatchSize, Duration fetchMaxWait) {
  //    return Flux.generate((SynchronousSink<List<Message>> sink) -> {
  //      var tr = Try.of(() -> sub.fetch(fetchBatchSize, fetchMaxWait));
  //      if (tr.isSuccess()) {
  //        List<Message> messages = List.ofAll(tr.get()).dropWhile(Message::isStatusMessage);
  //        messages.forEach(System.out::println);
  //        sink.next(messages);
  //      } else {
  //        sink.error(tr.getCause());
  //      }
  //    });
  //  }
  static Flux<Message> fetchMessagesForCmds(JetStreamSubscription sub, int fetchBatchSize, Duration fetchMaxWait) {
    //    return Mono.fromCallable(() -> Flux.fromIterable(sub.fetch(fetchBatchSize, fetchMaxWait)))
    //               .repeat()
    //               .flatMap(Function.identity())
    //               .map(message -> {
    //                 message.ack();
    //                 return message;
    //               });
    return Flux.generate((SynchronousSink<Flux<Message>> sink) -> {
      var tr = Try.of(() -> sub.fetch(fetchBatchSize, fetchMaxWait));
      if (tr.isSuccess()) {
        List<Message> messages = List.ofAll(tr.get()).dropWhile(Message::isStatusMessage);
        messages.forEach(System.out::println);
        System.out.println("fetchMessageForCmds is called size=" + messages.size());
        sink.next(Flux.fromIterable(messages));
      } else {
        sink.error(tr.getCause());
      }
    }).concatMap(Function.identity()).subscribeOn(Schedulers.boundedElastic());
  }

  static Flux<Message> fetchMessages(JetStreamSubscription sub, int fetchBatchSize, Duration fetchMaxWait) {
    return Mono.fromCallable(() -> Flux.fromIterable(sub.fetch(fetchBatchSize, fetchMaxWait)))
               .repeat()
               .flatMap(Function.identity())
               .map(message -> {
                 message.ack();
                 return message;
               });
    //    return Flux.generate((SynchronousSink<Flux<Message>> sink) -> {
    //      var tr = Try.of(() -> sub.fetch(fetchBatchSize, fetchMaxWait));
    //      if (tr.isSuccess()) {
    //        List<Message> messages = List.ofAll(tr.get()).dropWhile(Message::isStatusMessage);
    //        messages.forEach(System.out::println);
    //        sink.next(Flux.fromIterable(messages));
    //      } else {
    //        sink.error(tr.getCause());
    //      }
    //    }).concatMap(Function.identity());
  }

  static JetStreamSubscription jetStreamSub(JetStream js, DeliverPolicy deliverPolicy, String topic, int partition) {
    var config = ConsumerConfiguration.builder()
                                      .ackPolicy(AckPolicy.Explicit)
                                      .deliverPolicy(deliverPolicy)
                                      .replayPolicy(ReplayPolicy.Instant)
                                      .build();
    var streamName = streamName(topic, partition);
    var subscribeOptions = PullSubscribeOptions.builder().stream(streamName).configuration(config).build();
    try {
      String subjectName = subjectName(topic, partition);
      return js.subscribe(subjectName, subscribeOptions);
    } catch (IOException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  static StreamConfiguration streamConfiguration(NatsConfig natsConfig, String topic, int partition) {
    return StreamConfiguration.builder()
                              .replicas(natsConfig.replicas())
                              .storageType(natsConfig.storageType())
                              .denyDelete(natsConfig.denyDelete())
                              .denyPurge(natsConfig.denyPurge())
                              .name(streamName(topic, partition))
                              .subjects(subjectName(topic, partition))
                              .build();
  }

  static StreamInfo createOrUpdateStream(Connection nc, StreamConfiguration streamConfiguration)
          throws IOException, JetStreamApiException {
    var streamNames = nc.jetStreamManagement().getStreamNames();
    if (streamNames.contains(streamConfiguration.getName()))
      return nc.jetStreamManagement().updateStream(streamConfiguration);
    else
      return nc.jetStreamManagement().addStream(streamConfiguration);
  }

  static ErrorListener errorListener() {
    return new ErrorListener() {
      @Override
      public void errorOccurred(Connection conn, String error) {
        ErrorListener.super.errorOccurred(conn, error);
      }
    };
  }

  static String streamName(String topic, int partition) {
    return "%s%s%d".formatted(topic, SPLIT_TOKEN, partition);
  }
}
