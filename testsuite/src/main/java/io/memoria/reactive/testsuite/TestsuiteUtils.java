package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class TestsuiteUtils {

  public static final Scheduler SCHEDULER = Schedulers.boundedElastic();
  public static final int MSG_COUNT = 7773;
  public static final Duration TIMEOUT = Duration.ofMillis(500);
  public static final TextTransformer SERIALIZABLE_TRANSFORMER = new SerializableTransformer();

  public static String topicName(String postfix) {
    return "topic%d_%s".formatted(System.currentTimeMillis() / 1000, postfix);
  }

  public static String topicName(Class<?> tClass) {
    return "topic%d_%s".formatted(System.currentTimeMillis() / 1000, tClass.getSimpleName());
  }
}
