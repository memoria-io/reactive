package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;

import java.time.Duration;

public class TestUtils {
  public static final String NATS_URL = "nats://localhost:4222";

  private TestUtils() {}

  public static NatsConfig natsConfig() {
    return NatsConfig.appendOnly(NATS_URL, StorageType.File, 1, 100, Duration.ofMillis(100), Duration.ofMillis(300));
  }
}
