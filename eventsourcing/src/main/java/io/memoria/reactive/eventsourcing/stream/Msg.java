package io.memoria.reactive.eventsourcing.stream;

public record Msg(String key, String value) {
  public Msg(long key, String value) {
    this(String.valueOf(key), value);
  }

  public Msg {
    if (key == null || key.isBlank()) {
      throw new IllegalArgumentException("Key is null or blank");
    }
  }
}
