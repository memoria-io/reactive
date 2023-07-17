package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.AccountDecider;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.AccountEvolver;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.AccountSaga;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.Account;

import java.util.function.Supplier;

public class Infra {

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createMemoryPipeline(Supplier<Id> idSupplier,
                                                                                              Supplier<Long> timeSupplier) {
    var domain = stateDomain(idSupplier, timeSupplier);
    var commandRoute = new CommandRoute("commands", 0);
    var eventRoute = new EventRoute("events", 0);
    return new PartitionPipeline<>(domain, CommandStream.inMemory(), commandRoute, EventStream.inMemory(), eventRoute);
  }

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createMemoryPipeline(Supplier<Id> idSupplier,
                                                                                              Supplier<Long> timeSupplier,
                                                                                              CommandRoute commandRoute,
                                                                                              EventRoute eventRoute) {
    var domain = stateDomain(idSupplier, timeSupplier);
    return new PartitionPipeline<>(domain, CommandStream.inMemory(), commandRoute, EventStream.inMemory(), eventRoute);
  }

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createPipeline(Supplier<Id> idSupplier,
                                                                                        Supplier<Long> timeSupplier,
                                                                                        CommandStream<AccountCommand> commandStream,
                                                                                        CommandRoute commandRoute,
                                                                                        EventStream<AccountEvent> eventStream,
                                                                                        EventRoute eventRoute) {
    var domain = stateDomain(idSupplier, timeSupplier);
    return new PartitionPipeline<>(domain, commandStream, commandRoute, eventStream, eventRoute);
  }

  public static Domain<Account, AccountCommand, AccountEvent> stateDomain(Supplier<Id> idSupplier,
                                                                          Supplier<Long> timeSupplier) {
    return new Domain<>(Account.class,
                        AccountCommand.class,
                        AccountEvent.class,
                        new AccountDecider(idSupplier, timeSupplier),
                        new AccountEvolver(idSupplier, timeSupplier),
                        new AccountSaga(idSupplier, timeSupplier));
  }

  private Infra() {}
}
