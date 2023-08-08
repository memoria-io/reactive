package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.EventMeta;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.vavr.control.Try;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

public class PartitionPipeline<S extends State, C extends Command, E extends Event> {
  // Core
  public final Domain<S, C, E> domain;

  // Infra
  private final CommandStream<C> commandStream;
  private final CommandRoute commandRoute;

  public final EventStream<E> eventStream;
  public final EventRoute eventRoute;

  // In memory
  private final Map<StateId, S> aggregates;
  private final Set<CommandId> processedCommands;
  private final Set<EventId> processedEvents;

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
    return handle(commandStream.sub(commandRoute.topicName(), commandRoute.partition()));
  }

  public Flux<E> handle(Flux<C> cmds) {
    var handleCommands = cmds.concatMap(this::redirectIfNotBelong)
                             .concatMap(this::handleCommand)
                             .doOnNext(this::evolve)
                             .concatMap(this::saga)
                             .concatMap(this::pubEvent);
    return init().concatWith(handleCommands);
  }

  public Mono<C> pubCommand(C cmd) {
    return Mono.fromCallable(() -> cmd.meta().partition(commandRoute.totalPartitions()))
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

  /**
   * Load previous events and build the state
   */
  Flux<E> init() {
    return this.eventStream.last(eventRoute.topicName(), eventRoute.partition())
                           .map(E::meta)
                           .map(EventMeta::eventId)
                           .flatMapMany(this::subUntil)
                           .doOnNext(this::evolve);
  }

  /**
   * Redirection allows location transparency and auto sharding
   */
  Mono<C> redirectIfNotBelong(C cmd) {
    if (cmd.meta().isInPartition(commandRoute.partition(), commandRoute.totalPartitions())) {
      return Mono.just(cmd);
    } else {
      return this.pubCommand(cmd).flatMap(c -> Mono.empty());
    }
  }

  Mono<E> handleCommand(C cmd) {
    return Mono.defer(() -> {
      if (processedCommands.contains(cmd.meta().commandId())) {
        return Mono.empty();
      } else {
        return tryToMono(() -> decide(cmd));
      }
    });
  }

  Try<E> decide(C cmd) {
    if (aggregates.containsKey(cmd.meta().stateId())) {
      return domain.decider().apply(aggregates.get(cmd.meta().stateId()), cmd);
    } else {
      return domain.decider().apply(cmd);
    }
  }

  void evolve(E e) {
    if (!processedEvents.contains(e.meta().eventId())) {
      S newState;
      if (aggregates.containsKey(e.meta().stateId())) {
        newState = domain.evolver().apply(aggregates.get(e.meta().stateId()), e);
      } else {
        newState = domain.evolver().apply(e);
      }
      aggregates.put(e.meta().stateId(), newState);
      processedCommands.add(e.meta().commandId());
      processedEvents.add(e.meta().eventId());
    }
  }

  Mono<E> saga(E e) {
    return Mono.defer(() -> {
      var opt = domain.saga().apply(e);
      if (opt.isDefined()) {
        return this.pubCommand(opt.get()).map(c -> e);
      } else {
        return Mono.just(e);
      }
    });
  }
}