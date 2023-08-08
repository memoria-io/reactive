package io.memoria.reactive.testsuite.eventsourcing.banking;

import io.memoria.atom.core.id.Id;
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

import java.util.function.Supplier;

public class BankingInfra {

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createMemoryPipeline(Supplier<Id> idSupplier,
                                                                                              Supplier<Long> timeSupplier) {
    var domain = stateDomain(idSupplier, timeSupplier);

    var commandRoute = new CommandRoute("commands", 0);
    var eventRoute = new EventRoute("events", 0);

    var commandStream = CommandStream.inMemory(AccountCommand.class);
    var eventStream = EventStream.inMemory(AccountEvent.class);
    return new PartitionPipeline<>(domain, commandStream, commandRoute, eventStream, eventRoute);
  }

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createMemoryPipeline(Supplier<Id> idSupplier,
                                                                                              Supplier<Long> timeSupplier,
                                                                                              CommandRoute commandRoute,
                                                                                              EventRoute eventRoute) {
    var domain = stateDomain(idSupplier, timeSupplier);
    var commandStream = CommandStream.inMemory(AccountCommand.class);
    var eventStream = EventStream.inMemory(AccountEvent.class);
    return new PartitionPipeline<>(domain, commandStream, commandRoute, eventStream, eventRoute);
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

  private BankingInfra() {}
}
