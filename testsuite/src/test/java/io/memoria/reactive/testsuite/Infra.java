package io.memoria.reactive.testsuite;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.SerializableTransformer;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountDecider;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountEvolver;
import io.memoria.atom.testsuite.eventsourcing.banking.AccountSaga;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.function.Supplier;

public class Infra {

  private static final Logger log = LoggerFactory.getLogger(Infra.class.getName());
  public static final Scheduler SCHEDULER = Schedulers.boundedElastic();
  public static final int MSG_COUNT = 1000;
  public static final Duration TIMEOUT = Duration.ofMillis(500);
  public static final TextTransformer TRANSFORMER = new SerializableTransformer();

  public static Domain<Account, AccountCommand, AccountEvent> domain(Supplier<Id> idSupplier,
                                                                     Supplier<Long> timeSupplier) {
    return new Domain<>(Account.class,
                        AccountCommand.class,
                        AccountEvent.class,
                        new AccountDecider(idSupplier, timeSupplier),
                        new AccountEvolver(),
                        new AccountSaga(idSupplier, timeSupplier));
  }

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline(Supplier<Id> idSupplier,
                                                                                        Supplier<Long> timeSupplier,
                                                                                        CommandStream<AccountCommand> commandStream,
                                                                                        CommandRoute commandRoute,
                                                                                        EventStream<AccountEvent> eventStream,
                                                                                        EventRoute eventRoute) {
    var domain = domain(idSupplier, timeSupplier);
    return new PartitionPipeline<>(domain, commandStream, commandRoute, eventStream, eventRoute);
  }

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

  private Infra() {}
}
