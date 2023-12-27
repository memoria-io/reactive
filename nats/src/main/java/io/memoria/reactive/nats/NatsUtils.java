package io.memoria.reactive.nats;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerConfiguration.Builder;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.vavr.collection.List;
import io.vavr.control.Try;
import reactor.core.publisher.Flux;
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
  private NatsUtils() {}

  public static Connection createConnection(String url) throws IOException, InterruptedException {
    return Nats.connect(Options.builder().server(url).errorListener(errorListener()).build());
  }

  public static StreamInfo createOrUpdateStream(JetStreamManagement jsManagement, String topic, int replication)
          throws JetStreamApiException, IOException {
    return createOrUpdateStream(jsManagement, defaultCommandStreamConfig(topic, replication).build());
  }

  public static StreamInfo createOrUpdateStream(JetStreamManagement jsManagement,
                                                StreamConfiguration streamConfiguration)
          throws IOException, JetStreamApiException {
    var streamNames = jsManagement.getStreamNames();
    if (streamNames.contains(streamConfiguration.getName()))
      return jsManagement.updateStream(streamConfiguration);
    else
      return jsManagement.addStream(streamConfiguration);
  }

  public static StreamConfiguration.Builder defaultCommandStreamConfig(String topic, int replication) {
    return StreamConfiguration.builder()
                              .name(topic)
                              .subjects(toPartitionedSubjectName(topic))
                              .replicas(replication)
                              .storageType(StorageType.File)
                              .retentionPolicy(RetentionPolicy.WorkQueue)
                              .denyDelete(false)
                              .denyPurge(false);
  }

  public static Builder defaultCommandConsumerConfigs(String name) {
    return ConsumerConfiguration.builder()
                                .name(name)
                                .ackPolicy(AckPolicy.Explicit)
                                .deliverPolicy(DeliverPolicy.All)
                                .replayPolicy(ReplayPolicy.Instant);
  }

  public static String toPartitionedSubjectName(String topic) {
    return topic + ".*";
  }

  public static String toPartitionedSubjectName(String topic, int partition) {
    return topic + "." + partition;
  }

  public static String toSubscriptionName(String topic) {
    return "%s_%d_subscription".formatted(topic, System.currentTimeMillis());
  }

  public static Flux<Message> fetchMessages(JetStreamSubscription sub, int fetchBatchSize, Duration fetchMaxWait) {
    return Flux.generate((SynchronousSink<Flux<Message>> sink) -> {
      var tr = Try.of(() -> sub.fetch(fetchBatchSize, fetchMaxWait));
      if (tr.isSuccess()) {
        List<Message> messages = List.ofAll(tr.get()).dropWhile(Message::isStatusMessage).peek(Message::ack);
        System.out.println(messages);
        sink.next(Flux.fromIterable(messages));
      } else {
        sink.error(tr.getCause());
      }
    }).concatMap(Function.identity()).subscribeOn(Schedulers.boundedElastic());
  }

  private static ErrorListener errorListener() {
    return new ErrorListener() {
      @Override
      public void errorOccurred(Connection conn, String error) {
        ErrorListener.super.errorOccurred(conn, error);
      }
    };
  }

  static StreamInfo createOrUpdateStream(Connection nc, StreamConfiguration streamConfiguration)
          throws IOException, JetStreamApiException {
    var streamNames = nc.jetStreamManagement().getStreamNames();
    if (streamNames.contains(streamConfiguration.getName()))
      return nc.jetStreamManagement().updateStream(streamConfiguration);
    else
      return nc.jetStreamManagement().addStream(streamConfiguration);
  }
}
