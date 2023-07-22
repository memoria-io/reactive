package io.memoria.reactive.nats;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamOptions;
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

  private NatsUtils() {}

  public static Connection natsConnection(NatsConfig natsConfig) throws IOException, InterruptedException {
    return Nats.connect(Options.builder().server(natsConfig.url()).errorListener(errorListener()).build());
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

  public static JetStreamSubscription jetStreamSub(JetStream js, TopicConfig topicConfig, DeliverPolicy deliverPolicy) {
    var config = ConsumerConfiguration.builder()
                                      .ackPolicy(AckPolicy.Explicit)
                                      .deliverPolicy(deliverPolicy)
                                      .replayPolicy(ReplayPolicy.Instant)
                                      .build();
    var subscribeOptions = PullSubscribeOptions.builder()
                                               .stream(topicConfig.streamName())
                                               .configuration(config)
                                               .build();
    try {
      return js.subscribe(topicConfig.subjectName(), subscribeOptions);
    } catch (IOException | JetStreamApiException e) {
      throw new RuntimeException(e);
    }
  }

  public static StreamConfiguration toStreamConfiguration(TopicConfig c) {
    return StreamConfiguration.builder()
                              .replicas(c.replicas())
                              .storageType(c.storageType())
                              .denyDelete(c.denyDelete())
                              .denyPurge(c.denyPurge())
                              .name(c.streamName())
                              .subjects(c.subjectName())
                              .build();
  }

  public static List<Message> fetch(Connection connection, TopicConfig config) throws IOException {
    JetStreamOptions jso = JetStreamOptions.builder().optOut290ConsumerCreate(true).build();
    JetStream js = connection.jetStream(jso);
    var sub = jetStreamSub(js, config, DeliverPolicy.All);
    return List.ofAll(sub.fetch(config.fetchBatchSize(), config.fetchMaxWait()));
  }

  public static Flux<Message> fetch(JetStreamSubscription sub, TopicConfig config) {
    return Flux.generate((SynchronousSink<Flux<Message>> sink) -> {
      var tr = Try.of(() -> blockingFetch(sub, config));
      if (tr.isSuccess()) {
        sink.next(Flux.fromIterable(tr.get()));
      } else {
        sink.error(tr.getCause());
      }
    }).concatMap(Function.identity());
  }

  public static List<Message> blockingFetch(JetStreamSubscription sub, TopicConfig config) {
    System.out.println("blocking fetch called");
    return List.ofAll(sub.fetch(config.fetchBatchSize(), config.fetchMaxWait()));
  }

  public static Option<Message> blockingFetchLast(JetStreamSubscription sub, TopicConfig config) {
    return List.ofAll(sub.fetch(config.fetchBatchSize(), config.fetchMaxWait())).lastOption();
  }

  public static Message acknowledge(Message m) {
    System.out.println("ack_" + new String(m.getData()));
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
}
