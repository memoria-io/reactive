package io.memoria.reactive.eventsourcing.rule;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.CommandMeta;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.EventId;
import io.memoria.reactive.eventsourcing.StateId;
import io.vavr.Function1;
import io.vavr.control.Option;

import java.util.function.Supplier;

public interface Saga<E extends Event, C extends Command> extends Function1<E, Option<C>> {
  Supplier<Id> idSupplier();

  Supplier<Long> timeSupplier();

  default CommandMeta commandMeta(StateId stateId, EventId sagaSource) {
    return new CommandMeta(CommandId.of(idSupplier().get()), stateId, timeSupplier().get(), Option.some(sagaSource));
  }
}
