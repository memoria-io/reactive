package io.memoria.reactive.eventsourcing.pipeline;

public record CommandRoute(String topic, int totalPartitions) {
  public CommandRoute(String topic) {
    this(topic, 1);
  }

  public CommandRoute {
    if (topic == null || topic.isBlank()) {
      throw new IllegalArgumentException("Invalid topic name");
    }
    if (totalPartitions < 1) {
      String msg = "Invalid total partitions %d value".formatted(totalPartitions);
      throw new IllegalArgumentException(msg);
    }
  }
}
