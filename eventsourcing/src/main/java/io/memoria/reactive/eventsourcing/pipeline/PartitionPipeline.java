package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.caching.KCache;
import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.ESException.InvalidEvent;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.EventMeta;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.memoria.reactive.core.reactor.ReactorUtils.tryToMono;

public class PartitionPipeline<S extends State, C extends Command, E extends Event> {
  private static final Logger log = LoggerFactory.getLogger(PartitionPipeline.class.getName());

  // Core
  public final Domain<S, C, E> domain;

  // Infra
  private final CommandStream<C> commandStream;
  private final CommandRoute commandRoute;

  private final EventStream<E> eventStream;
  private final EventRoute eventRoute;

  // In memory
  private final Map<StateId, S> aggregates;
  private final KCache<CommandId> processedCommands;
  private final AtomicReference<EventId> prevEvent;

  /**
   * Create pipeline with default commandId cache size of 1Million ~= 16Megabyte, since UUID is 32bit -> 16byte
   */
  public PartitionPipeline(Domain<S, C, E> domain,
                           CommandStream<C> commandStream,
                           CommandRoute commandRoute,
                           EventStream<E> eventStream,
                           EventRoute eventRoute,
                           int cacheCapacity) {
    this(domain, commandStream, commandRoute, eventStream, eventRoute, KCache.inMemory(cacheCapacity));
  }

  public PartitionPipeline(Domain<S, C, E> domain,
                           CommandStream<C> commandStream,
                           CommandRoute commandRoute,
                           EventStream<E> eventStream,
                           EventRoute eventRoute,
                           KCache<CommandId> commandIdCache) {
    // Core
    this.domain = domain;

    // Infra
    this.commandStream = commandStream;
    this.commandRoute = commandRoute;

    this.eventStream = eventStream;
    this.eventRoute = eventRoute;

    // In memory
    this.aggregates = new HashMap<>();
    this.processedCommands = commandIdCache;
    this.prevEvent = new AtomicReference<>();
  }

  public Flux<E> handle() {
    return handle(commandStream.sub(commandRoute.name(), commandRoute.partition()));
  }

  public Flux<E> handle(Flux<C> cmds) {
    var handleCommands = cmds.concatMap(this::redirectIfNeeded)
                             .concatMap(this::decide)
                             .doOnNext(this::evolve)
                             .concatMap(this::saga)
                             .concatMap(this::pubEvent);
    return init().concatWith(handleCommands);
  }

  public Mono<C> pubCommand(C cmd) {
    return Mono.fromCallable(() -> cmd.meta().partition(commandRoute.totalPartitions()))
               .flatMap(partition -> commandStream.pub(commandRoute.name(), partition, cmd));
  }

  public Flux<C> subToCommands() {
    return commandStream.sub(commandRoute.name(), commandRoute.partition());
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
  Mono<C> redirectIfNeeded(C cmd) {
    if (cmd.meta().isInPartition(commandRoute.partition(), commandRoute.totalPartitions())) {
      return Mono.just(cmd);
    } else {
      return this.pubCommand(cmd).flatMap(c -> Mono.empty());
    }
  }

  Mono<E> decide(C cmd) {
    return Mono.defer(() -> {
      if (processedCommands.contains(cmd.meta().commandId())) {
        return Mono.empty();
      } else {
        if (aggregates.containsKey(cmd.meta().stateId())) {
          return tryToMono(() -> domain.decider().apply(aggregates.get(cmd.meta().stateId()), cmd));
        } else {
          return tryToMono(() -> domain.decider().apply(cmd));
        }
      }
    });
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

  Mono<E> pubEvent(E e) {
    return eventStream.pub(eventRoute.topicName(), eventRoute.partition(), e);
  }

  void evolve(E e) {
    if (aggregates.containsKey(e.meta().stateId())) {
      S currentState = aggregates.get(e.meta().stateId());
      if (hasExpectedVersion(e, currentState)) {
        update(e, domain.evolver().apply(currentState, e));
      } else if (isDuplicate(e)) {
        String message = "Redelivered event[%s], ignoring...".formatted(e.meta());
        log.debug(message);
      } else {
        throw InvalidEvent.of(currentState, e);
      }
    } else if (e.meta().version() == 0) {
      update(e, domain.evolver().apply(e));
    } else {
      throw InvalidEvent.of(e);
    }
  }

  boolean hasExpectedVersion(E e, S currentState) {
    return e.meta().version() == currentState.meta().version() + 1;
  }

  boolean isDuplicate(E e) {
    return prevEvent.get() == null && prevEvent.get().equals(e.meta().eventId());
  }

  void update(E e, S newState) {
    aggregates.put(e.meta().stateId(), newState);
    processedCommands.add(e.meta().commandId());
    prevEvent.set(e.meta().eventId());
  }
}