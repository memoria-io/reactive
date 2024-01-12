package io.memoria.reactive.eventsourcing.pipeline;

public record EventRoute(String topic, int partition, int totalPartitions) {
  public EventRoute(String topic) {
    this(topic, 0, 1);
  }

  public EventRoute {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("Invalid topic name");
    }
    if (partition < 0 || partition >= totalPartitions) {
      String msg = "Invalid partition number %d or total partitions %d".formatted(partition, totalPartitions);
      throw new IllegalArgumentException(msg);
    }
  }
}
