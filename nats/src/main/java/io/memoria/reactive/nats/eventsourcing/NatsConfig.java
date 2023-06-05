package io.memoria.reactive.nats.eventsourcing;

import io.vavr.collection.HashSet;
import io.vavr.collection.Set;
import io.vavr.control.Option;

public record NatsConfig(String url, Set<TopicConfig> configs) {

  public NatsConfig(String url, TopicConfig... topic) {
    this(url, HashSet.of(topic));
  }

  public Option<TopicConfig> find(String name, int partition) {
    return configs.find(tp -> tp.topic().equals(name) && tp.partition() == partition);
  }
}
