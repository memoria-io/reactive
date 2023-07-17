package io.memoria.reactive.eventsourcing.pipeline.partition;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.State;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.memoria.reactive.core.reactor.ReactorUtils.booleanToMono;
import static io.memoria.reactive.core.reactor.ReactorUtils.optionToMono;

public class PartitionPipeline<S extends State, C extends Command, E extends Event> {
  // Core
  public final Domain<S, C, E> domain;

  // Infra
  private final CommandStream<C> commandStream;
  private final CommandRoute commandRoute;

  public final EventStream<E> eventStream;
  public final EventRoute eventRoute;

  // In memory
  private final Map<Id, S> aggregates;
  private final Set<Id> processedCommands;
  private final Set<Id> processedEvents;

  public PartitionPipeline(Domain<S, C, E> domain,
                           CommandStream<C> commandStream,
                           CommandRoute commandRoute,
                           EventStream<E> eventStream,
                           EventRoute eventRoute) {
    // Core
    this.domain = domain;

    // Infra
    this.commandStream = commandStream;
    this.commandRoute = commandRoute;

    this.eventStream = eventStream;
    this.eventRoute = eventRoute;

    // In memory
    this.aggregates = new HashMap<>();
    this.processedCommands = new HashSet<>();
    this.processedEvents = new HashSet<>();
  }

  public Flux<E> handle() {
    return handle(commandStream.sub(commandRoute.name(), commandRoute.partition()));
  }

  public Flux<E> handle(Flux<C> cmds) {
    var handleCommands = cmds.concatMap(this::redirectIfNotBelong) // Redirection allows location transparency and auto sharding
                             .concatMap(this::handleCommand) // handle the command
                             .concatMap(this::evolve) // evolve the state
                             .concatMap(this::pubEvent) // publish the event
                             .concatMap(this::saga); // publish saga command;
    return init().concatWith(handleCommands);
  }

  public Mono<C> pubCommand(C cmd) {
    return Mono.fromCallable(() -> cmd.partition(commandRoute.totalPartitions()))
               .flatMap(partition -> commandStream.pub(commandRoute.name(), partition, cmd));
  }

  public Flux<C> subToCommands() {
    return commandStream.sub(commandRoute.name(), commandRoute.partition());
  }

  public Mono<E> pubEvent(E e) {
    return eventStream.pub(eventRoute.name(), eventRoute.partition(), e);
  }

  public Flux<E> subToEvents() {
    return eventStream.sub(eventRoute.name(), eventRoute.partition());
  }

  public Flux<E> subUntil(Id id) {
    return eventStream.subUntil(eventRoute.name(), eventRoute.partition(), id);
  }

  /**
   * Load previous events and build the state
   */
  Flux<E> init() {
    return this.eventStream.last(eventRoute.name(), eventRoute.partition())
                           .flatMapMany(this::subUntil)
                           .concatMap(this::evolve);
  }

  Mono<C> redirectIfNotBelong(C cmd) {
    if (cmd.isInPartition(commandRoute.partition(), commandRoute.totalPartitions())) {
      return Mono.just(cmd);
    } else {
      return this.pubCommand(cmd).flatMap(c -> Mono.empty());
    }
  }

  Mono<E> handleCommand(C cmd) {
    return Mono.fromCallable(() -> processedCommands.contains(cmd.commandId()))
               .flatMap(exists -> booleanToMono(!exists, decide(cmd)));
  }

  private Mono<E> decide(C cmd) {
    return Mono.fromCallable(() -> aggregates.containsKey(cmd.stateId()))
               .flatMap(stateExists -> booleanToMono(stateExists, decideWithState(cmd), decideWithoutState(cmd)));
  }

  private Mono<E> decideWithoutState(C cmd) {
    return ReactorUtils.tryToMono(() -> domain.decider().apply(cmd));
  }

  private Mono<E> decideWithState(C cmd) {
    return ReactorUtils.tryToMono(() -> domain.decider().apply(aggregates.get(cmd.stateId()), cmd));
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
      if (aggregates.containsKey(e.stateId())) {
        newState = domain.evolver().apply(aggregates.get(e.stateId()), e);
      } else {
        newState = domain.evolver().apply(e);
      }
      aggregates.put(e.stateId(), newState);
      processedCommands.add(e.commandId());
      processedEvents.add(e.eventId());
      return e;
    });
  }
}