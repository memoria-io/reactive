package io.memoria.reactive.eventsourcing.pipeline.aggregate;

import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.EventId;
import io.memoria.reactive.eventsourcing.State;
import io.memoria.reactive.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.pipeline.partition.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.partition.EventRoute;
import io.memoria.reactive.eventsourcing.port.EventPort;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static io.memoria.reactive.core.reactor.ReactorUtils.booleanToMono;
import static io.memoria.reactive.core.reactor.ReactorUtils.optionToMono;

/**
 * Work in progress
 */
public class Aggregate<S extends State, C extends Command, E extends Event> {
  // Core
  public final Domain<S, C, E> domain;

  // Infra
  private final CommandStream<C> commandStream;
  private final CommandRoute commandRoute;

  public final EventPort<E> eventPort;
  public final String eventPortTable;
  public final EventStream<E> eventStream;
  public final EventRoute eventRoute;

  // In memory
  private final AtomicReference<S> aggregate;
  private final Set<CommandId> processedCommands;
  private final Set<EventId> processedEvents;

  public Aggregate(Domain<S, C, E> domain,
                   CommandStream<C> commandStream,
                   CommandRoute commandRoute,
                   EventPort<E> eventPort,
                   String eventPortTable,
                   EventStream<E> eventStream,
                   EventRoute eventRoute) {
    // Core
    this.domain = domain;

    // Infra
    this.commandStream = commandStream;
    this.commandRoute = commandRoute;

    this.eventPort = eventPort;
    this.eventPortTable = eventPortTable;
    this.eventStream = eventStream;
    this.eventRoute = eventRoute;

    // In memory
    this.aggregate = new AtomicReference<>();
    this.processedCommands = new HashSet<>();
    this.processedEvents = new HashSet<>();
  }

  /**
   * Load previous events and build the state
   */
  public Flux<E> init(StateId stateId) {
    return eventPort.events(eventPortTable, stateId).concatMap(this::evolve);
  }

  public Flux<E> handle(Flux<C> commands) {
    return commands.concatMap(this::handleCommand) // handle the command
                   .concatMap(this::evolve) // evolve the state
                   .concatMap(this::pubEvent) // publish the event
                   .concatMap(this::saga);
  }

  public Mono<C> pubCommand(C cmd) {
    return Mono.fromCallable(() -> cmd.partition(commandRoute.totalPartitions()))
               .flatMap(partition -> commandStream.pub(commandRoute.topicName(), partition, cmd));
  }

  public Flux<C> subToCommands() {
    return commandStream.sub(commandRoute.topicName(), commandRoute.partition());
  }

  public Mono<E> pubEvent(E e) {
    return eventStream.pub(eventRoute.topicName(), eventRoute.partition(), e);
  }

  public Flux<E> subToEvents() {
    return eventStream.sub(eventRoute.topicName(), eventRoute.partition());
  }

  public Flux<E> subUntil(EventId id) {
    return eventStream.subUntil(eventRoute.topicName(), eventRoute.partition(), id);
  }

  Mono<E> handleCommand(C cmd) {
    return Mono.fromCallable(() -> validate(cmd)).flatMap(exists -> booleanToMono(!exists, decide(cmd)));
  }

  boolean validate(C cmd) {
    return !processedCommands.contains(cmd.commandId()) && hasCorrectState(cmd) && isInPartition(cmd);
  }

  boolean hasCorrectState(C cmd) {
    return this.aggregate.get().stateId().equals(cmd.stateId());
  }

  boolean isInPartition(C cmd) {
    return cmd.isInPartition(commandRoute.partition(), commandRoute.totalPartitions());
  }

  Mono<E> decide(C cmd) {
    return Mono.fromCallable(() -> aggregate.get() != null)
               .flatMap(stateExists -> booleanToMono(stateExists, decideWithState(cmd), decideWithoutState(cmd)));
  }

  Mono<E> decideWithoutState(C cmd) {
    return ReactorUtils.tryToMono(() -> domain.decider().apply(cmd));
  }

  Mono<E> decideWithState(C cmd) {
    return ReactorUtils.tryToMono(() -> domain.decider().apply(aggregate.get(), cmd));
  }

  Mono<E> evolve(E e) {
    return Mono.fromCallable(() -> processedEvents.contains(e.eventId()))
               .flatMap(exists -> booleanToMono(!exists, handleEvent(e)));
  }

  Mono<E> saga(E e) {
    return optionToMono(domain.saga().apply(e)).flatMap(this::pubCommand).map(c -> e).defaultIfEmpty(e);
  }

  Mono<E> handleEvent(E e) {
    return Mono.fromCallable(() -> {
      S newState;
      if (aggregate.get() == null) {
        newState = domain.evolver().apply(e);
      } else {
        newState = domain.evolver().apply(aggregate.get(), e);
      }
      aggregate.set(newState);
      processedCommands.add(e.commandId());
      processedEvents.add(e.eventId());
      return e;
    });
  }
}