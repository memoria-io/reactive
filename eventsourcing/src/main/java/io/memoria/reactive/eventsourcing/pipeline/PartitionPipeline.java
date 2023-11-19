package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.ESException.InvalidEvent;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.EventId;
import io.memoria.reactive.eventsourcing.EventMeta;
import io.memoria.reactive.eventsourcing.State;
import io.memoria.reactive.eventsourcing.StateId;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
  private final Set<CommandId> processedCommands;
  private final Set<EventId> sagaSources;
  private final AtomicReference<EventId> prevEvent;

  // Config
  private final boolean startupSagaEnabled;

  /**
   * The Ids cache (sagaSources and processedCommands) size of 1Million ~= 16Megabyte, since UUID is 32bit -> 16byte
   *
   * @param startupSagaEnabled this enables self-healing by reproducing saga commands, adapting to state-machine new
   *                           additions, note the reproduced saga commands have no effect if they were already ingested
   *                           and produced an event.
   */
  public PartitionPipeline(Domain<S, C, E> domain,
                           CommandStream<C> commandStream,
                           CommandRoute commandRoute,
                           EventStream<E> eventStream,
                           EventRoute eventRoute,
                           boolean startupSagaEnabled) {
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
    this.sagaSources = new HashSet<>();
    this.prevEvent = new AtomicReference<>();

    // Config
    this.startupSagaEnabled = startupSagaEnabled;
  }

  public Flux<E> handle() {
    return handle(commandStream.sub(commandRoute.name(), commandRoute.partition()));
  }

  public Flux<E> handle(Flux<C> cmds) {
    var handleCommands = cmds.concatMap(this::redirectIfNeeded)
                             .concatMap(this::decide)
                             .doOnNext(this::evolve)
                             .concatMap(this::saga)
                             .concatMap(this::publish);
    return initialize().concatWith(handleCommands);
  }

  public Mono<C> publish(C cmd) {
    return Mono.fromCallable(() -> cmd.meta().partition(commandRoute.totalPartitions()))
               .flatMap(partition -> commandStream.pub(commandRoute.name(), partition, cmd));
  }

  public Flux<E> subToEvents() {
    return eventStream.sub(eventRoute.topicName(), eventRoute.partition());
  }

  public Flux<E> subToEventsUntil(EventId id) {
    return eventStream.subUntil(eventRoute.topicName(), eventRoute.partition(), id);
  }

  Mono<E> publish(E e) {
    return eventStream.pub(eventRoute.topicName(), eventRoute.partition(), e);
  }

  /**
   * Load previous events and build the state
   */
  Flux<E> initialize() {
    return this.eventStream.last(eventRoute.topicName(), eventRoute.partition())
                           .map(E::meta)
                           .map(EventMeta::eventId)
                           .flatMapMany(this::subToEventsUntil)
                           .doOnNext(this::evolve)
                           .concatMap(this::startupSaga);
  }

  /**
   * Redirection allows location transparency and auto sharding
   */
  Mono<C> redirectIfNeeded(C cmd) {
    if (cmd.meta().isInPartition(commandRoute.partition(), commandRoute.totalPartitions())) {
      return Mono.just(cmd);
    } else {
      return this.publish(cmd).flatMap(c -> Mono.empty());
    }
  }

  Mono<E> decide(C cmd) {
    return Mono.defer(() -> {
      if (isHandledCommand(cmd) || isHandledSagaCommand(cmd)) {
        return Mono.empty();
      } else {
        cmd.meta().sagaSource().forEach(this.sagaSources::add);
        if (aggregates.containsKey(cmd.meta().stateId())) {
          return tryToMono(() -> domain.decider().apply(aggregates.get(cmd.meta().stateId()), cmd));
        } else {
          return tryToMono(() -> domain.decider().apply(cmd));
        }
      }
    });
  }

  Mono<E> startupSaga(E e) {
    return (startupSagaEnabled) ? saga(e) : Mono.just(e);
  }

  Mono<E> saga(E e) {
    return Mono.defer(() -> {
      var opt = domain.saga().apply(e);
      if (opt.isDefined()) {
        return this.publish(opt.get()).map(c -> e);
      } else {
        return Mono.just(e);
      }
    });
  }

  void evolve(E e) {
    e.meta().sagaSource().forEach(sagaSources::add);
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

  boolean isHandledCommand(C cmd) {
    return processedCommands.contains(cmd.meta().commandId());
  }

  boolean isHandledSagaCommand(C cmd) {
    return cmd.meta().sagaSource().map(sagaSources::contains).getOrElse(false);
  }
}