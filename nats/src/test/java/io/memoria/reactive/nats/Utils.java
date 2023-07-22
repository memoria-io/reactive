package io.memoria.reactive.nats;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.api.StorageType;
import io.vavr.collection.List;
import io.vavr.collection.Set;

import java.io.IOException;
import java.time.Duration;

public class Utils {
  public static final String NATS_URL = "nats://localhost:4222";

  private Utils() {}

  public static Set<TopicConfig> createConfig(String topic, int nTotalPartitions) {
    return List.range(0, nTotalPartitions).map(partition -> createTopicConfig(topic, partition)).toSet();
  }

  static TopicConfig createTopicConfig(String topic, int partition) {
    return TopicConfig.appendOnly(topic,
                                  partition,
                                  StorageType.File,
                                  1,
                                  100,
                                  Duration.ofMillis(100),
                                  Duration.ofMillis(300));
  }


}
