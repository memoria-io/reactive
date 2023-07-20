package io.memoria.reactive.nats;

import io.vavr.collection.HashSet;
import io.vavr.collection.List;
import io.vavr.collection.Set;
import io.vavr.control.Option;

public record NatsConfig(String url, Set<TopicConfig> configs) {
  public NatsConfig(String url, List<TopicConfig> topicConfig) {
    this(url, topicConfig.toSet());
  }

  public NatsConfig(String url, TopicConfig... topicConfig) {
    this(url, HashSet.of(topicConfig));
  }

  public Option<TopicConfig> find(String name, int partition) {
    return configs.find(tp -> tp.topic().equals(name) && tp.partition() == partition);
  }
}
