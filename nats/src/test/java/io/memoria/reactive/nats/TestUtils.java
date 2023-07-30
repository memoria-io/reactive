package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;

import java.time.Duration;

public class TestUtils {
  public static final String NATS_URL = "nats://localhost:4222";
  public static final int COUNT = 100000;
  public static final Duration timeout = Duration.ofMillis(500);

  private TestUtils() {}

  public static NatsConfig natsConfig() {
    return NatsConfig.appendOnly(NATS_URL, StorageType.File, 1, 1000, Duration.ofMillis(100), Duration.ofMillis(300));
  }
}
