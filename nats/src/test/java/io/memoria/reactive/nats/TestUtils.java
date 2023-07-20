package io.memoria.reactive.nats;

import io.memoria.reactive.core.messaging.stream.ESMsg;
import io.nats.client.api.StorageType;
import io.vavr.collection.List;

import java.time.Duration;

public class TestUtils {
  private TestUtils() {}

  public static TopicConfig createTopicConfig(String topic, int partition) {
    return TopicConfig.appendOnly(topic, partition, StorageType.File, 1, 100, Duration.ofMillis(500));
  }

  public static TopicConfig[] createConfigs(String topic, int nTotalPartitions) {
    return List.range(0, nTotalPartitions)
               .map(partition -> createTopicConfig(topic, partition))
               .toJavaArray(TopicConfig[]::new);
  }

  public static ESMsg createEsMsg(String topic, int partition, String key) {
    return new ESMsg(topic, partition, key, "hello_" + key);
  }
}
