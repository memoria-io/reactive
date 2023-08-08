package io.memoria.reactive.testsuite;

import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class Utils {
  private static final Logger log = LoggerFactory.getLogger(Utils.class.getName());
  public static final Scheduler SCHEDULER = Schedulers.boundedElastic();
  public static final int MSG_COUNT = 1000;
  public static final Duration TIMEOUT = Duration.ofMillis(500);
  public static final TextTransformer TRANSFORMER = new SerializableTransformer();

  public static String topicName(String postfix) {
    return "topic%d_%s".formatted(System.currentTimeMillis(), postfix);
  }

  public static String topicName(Class<?> tClass) {
    return "topic%d_%s".formatted(System.currentTimeMillis(), tClass.getSimpleName());
  }

  public static void printRates(String methodName, long now) {
    long totalElapsed = System.currentTimeMillis() - now;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, MSG_COUNT, totalElapsed);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec(totalElapsed));
  }

  private static double eventsPerSec(long totalElapsed) {
    return MSG_COUNT / (totalElapsed / 1000d);
  }

  private Utils() {}
}
