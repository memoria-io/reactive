package io.memoria.reactive.eventsourcing.pipeline;

public record CommandRoute(String topic, int partition, int totalPartitions) {
  public CommandRoute(String topic) {
    this(topic, 0, 1);
  }

  public CommandRoute {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("Invalid topic name");
    }
    if (partition < 0 || partition >= totalPartitions) {
      String msg = "Invalid partition number %d or total partitions %d".formatted(partition, totalPartitions);
      throw new IllegalArgumentException(msg);
    }
  }
}
