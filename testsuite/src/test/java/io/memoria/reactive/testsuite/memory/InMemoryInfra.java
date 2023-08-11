package io.memoria.reactive.testsuite.memory;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.testsuite.eventsourcing.banking.command.AccountCommand;
import io.memoria.atom.testsuite.eventsourcing.banking.event.AccountEvent;
import io.memoria.atom.testsuite.eventsourcing.banking.state.Account;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.pipeline.PartitionPipeline;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.testsuite.Infra;

import java.util.function.Supplier;

class InMemoryInfra {

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createMemoryPipeline(Supplier<Id> idSupplier,
                                                                                              Supplier<Long> timeSupplier,
                                                                                              CommandRoute commandRoute,
                                                                                              EventRoute eventRoute) {
    var domain = Infra.domain(idSupplier, timeSupplier);
    var commandStream = CommandStream.inMemory(AccountCommand.class);
    var eventStream = EventStream.inMemory(AccountEvent.class);
    return new PartitionPipeline<>(domain, commandStream, commandRoute, eventStream, eventRoute);
  }

  public static PartitionPipeline<Account, AccountCommand, AccountEvent> createMemoryPipeline(Supplier<Id> idSupplier,
                                                                                              Supplier<Long> timeSupplier) {
    var domain = Infra.domain(idSupplier, timeSupplier);

    var commandRoute = new CommandRoute("commands", 0);
    var eventRoute = new EventRoute("events", 0);

    var commandStream = CommandStream.inMemory(AccountCommand.class);
    var eventStream = EventStream.inMemory(AccountEvent.class);
    return new PartitionPipeline<>(domain, commandStream, commandRoute, eventStream, eventRoute);
  }
}
