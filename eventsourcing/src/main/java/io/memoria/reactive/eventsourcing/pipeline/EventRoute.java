package io.memoria.reactive.eventsourcing.pipeline;

import java.util.Objects;

public record EventRoute(String topic, int partition, int totalPartitions) {
  public EventRoute(String topic) {
    this(topic, 0, 1);
  }

  public EventRoute {
    Objects.requireNonNull(topic);
    if (topic.isBlank() || topic.contains(" ")) {
      throw new IllegalArgumentException("Invalid topic name %s".formatted(topic));
    }
    if (partition < 0) {
      throw new IllegalArgumentException("Partition is below zero");
    }
    if (partition >= totalPartitions) {
      throw new IllegalArgumentException("Partition is more than totalPartitions");
    }
  }
}
