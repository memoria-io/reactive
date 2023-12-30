package io.memoria.reactive.testsuite;

import org.slf4j.Logger;

public class Utils {
  private Utils(){}

  public static void printRates(Logger log , String methodName, long start, long msgCount) {
    long totalElapsed = System.currentTimeMillis() - start;
    log.info("{}: Finished processing {} events, in {} millis %n", methodName, msgCount, totalElapsed);
    var eventsPerSec = msgCount / (totalElapsed / 1000d);
    log.info("{}: Average {} events per second %n", methodName, (long) eventsPerSec);
  }
}
