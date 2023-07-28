package io.memoria.reactive.nats;

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
import io.vavr.control.Option;
import io.vavr.control.Try;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
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

  public static Connection natsConnection(NatsConfig natsConfig) throws IOException, InterruptedException {
    return Nats.connect(Options.builder().server(natsConfig.url()).errorListener(errorListener()).build());
  }

  public static StreamConfiguration toStreamConfiguration(NatsConfig c, String topic, int partition) {
    return StreamConfiguration.builder()
                              .replicas(c.replicas())
                              .storageType(c.storageType())
                              .denyDelete(c.denyDelete())
                              .denyPurge(c.denyPurge())
                              .name(streamName(topic, partition))
                              .subjects(subjectName(topic, partition))
                              .build();
  }

  public static Try<StreamInfo> createOrUpdateStream(Connection nc, StreamConfiguration streamConfiguration) {
    return Try.of(() -> {
      var streamNames = nc.jetStreamManagement().getStreamNames();
      if (streamNames.contains(streamConfiguration.getName()))
        return nc.jetStreamManagement().updateStream(streamConfiguration);
      else
        return nc.jetStreamManagement().addStream(streamConfiguration);
    });
  }

  public static JetStreamSubscription jetStreamSub(JetStream js,
                                                   DeliverPolicy deliverPolicy,
                                                   String topic,
                                                   int partition) {
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

  public static Flux<Message> fetch(JetStreamSubscription sub, NatsConfig config) {
    return Flux.generate((SynchronousSink<Flux<Message>> sink) -> {
      var tr = Try.of(() -> blockingFetch(sub, config));
      if (tr.isSuccess()) {
        sink.next(Flux.fromIterable(tr.get()));
      } else {
        sink.error(tr.getCause());
      }
    }).concatMap(Function.identity());
  }

  public static List<Message> blockingFetch(JetStreamSubscription sub, NatsConfig config) {
    return List.ofAll(sub.fetch(config.fetchBatchSize(), config.fetchMaxWait()));
  }

  public static Option<Message> blockingFetchLast(JetStreamSubscription sub, NatsConfig config) {
    return List.ofAll(sub.fetch(config.fetchBatchSize(), config.fetchMaxWait())).lastOption();
  }

  public static Message acknowledge(Message m) {
    m.ack();
    return m;
  }

  public static ErrorListener errorListener() {
    return new ErrorListener() {
      @Override
      public void errorOccurred(Connection conn, String error) {
        ErrorListener.super.errorOccurred(conn, error);
      }
    };
  }

  public static String streamName(String topic, int partition) {
    return "%s%s%d".formatted(topic, SPLIT_TOKEN, partition);
  }

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
}
