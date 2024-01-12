package io.memoria.reactive.nats;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerConfiguration.Builder;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.io.IOException;

/**
 * Utility class for directly using nats, this layer is for testing NATS java driver, and ideally it shouldn't have
 * Flux/Mono utilities, only pure java/nats APIs
 */
public class NatsUtils {
  private NatsUtils() {}

  public static Connection createConnection(String url) throws IOException, InterruptedException {
    return Nats.connect(Options.builder().server(url).errorListener(errorListener()).build());
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

  public static StreamConfiguration.Builder defaultStreamConfig(String topic, int replication) {
    return StreamConfiguration.builder()
                              .name(topic)
                              .subjects(topic + ".*")
                              .replicas(replication)
                              .storageType(StorageType.File)
                              .denyDelete(false)
                              .denyPurge(false);
  }

  public static Builder defaultConsumerConfigs(String name) {
    return ConsumerConfiguration.builder()
                                .name(name)
                                .ackPolicy(AckPolicy.Explicit)
                                .replayPolicy(ReplayPolicy.Instant);
  }

  public static String toPartitionedSubjectName(String topic, int partition) {
    return topic + "." + partition;
  }

  public static String toSubscriptionName(String topic, int partition) {
    return "%s-%d-%d-subscription".formatted(topic, partition, System.currentTimeMillis());
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
