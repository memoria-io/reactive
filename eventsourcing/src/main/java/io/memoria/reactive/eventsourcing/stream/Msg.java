package io.memoria.reactive.eventsourcing.stream;

public record Msg(String topic, int partition, String key, String value) {
  public Msg(String topic, int partition, long key, String value) {
    this(topic, partition, String.valueOf(key), value);
  }

  public Msg {
    if (key == null || key.isBlank()) {
      throw new IllegalArgumentException("Key is null or blank");
    }
  }
}
