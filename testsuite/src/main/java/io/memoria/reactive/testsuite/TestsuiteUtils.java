package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.message.stream.ESMsg;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class TestsuiteUtils {

  public static final Scheduler SCHEDULER = Schedulers.boundedElastic();
  public static final int MSG_COUNT = 10003;
  public static final Duration TIMEOUT = Duration.ofMillis(500);
  public static final TextTransformer TRANSFORMER = new SerializableTransformer();

  public static String topicName(String postfix) {
    return "topic%d_%s".formatted(System.currentTimeMillis(), postfix);
  }

  public static String topicName(Class<?> tClass) {
    return "topic%d_%s".formatted(System.currentTimeMillis(), tClass.getSimpleName());
  }

  public static ESMsg createEsMsg(int i) {
    return new ESMsg(String.valueOf(i), "hello" + i);
  }
}
