package io.memoria.reactive.nats;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.nats.client.api.StorageType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class TestUtils {
  public static final String NATS_URL = "nats://localhost:4222";
  public static final int MSG_COUNT = 7773;
  public static final Duration TIMEOUT = Duration.ofMillis(500);
  public static final NatsConfig natsConfig = natsConfig();
  public static final Scheduler scheduler = Schedulers.boundedElastic();
  public static final TextTransformer transformer = new SerializableTransformer();

  public static String topicName(Class<?> tClass) {
    return "topic%d_%s".formatted(System.currentTimeMillis() / 1000, tClass.getSimpleName());
  }

  private TestUtils() {}

  private static NatsConfig natsConfig() {
    return NatsConfig.appendOnly(NATS_URL, StorageType.File, 1, 1000, Duration.ofMillis(100), Duration.ofMillis(300));
  }
}
