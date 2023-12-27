package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.eventsourcing.Command;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.Domain;
import io.memoria.atom.eventsourcing.Event;
import io.memoria.atom.eventsourcing.EventId;
import io.memoria.atom.eventsourcing.EventMeta;
import io.memoria.atom.eventsourcing.State;
import io.memoria.atom.eventsourcing.StateId;
import io.memoria.atom.eventsourcing.exceptions.InvalidEvolution;
import io.memoria.reactive.eventsourcing.stream.CommandRepo;
import io.memoria.reactive.eventsourcing.stream.EventRepo;
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

public class PartitionPipeline {
  private static final Logger log = LoggerFactory.getLogger(PartitionPipeline.class.getName());

  // Core
  public final Domain domain;

  // Infra
  public final CommandRepo commandRepo;
  public final EventRepo eventRepo;
  public final int partition;

  // In memory
  private final Map<StateId, State> aggregates;
  private final Set<CommandId> processedCommands;
  private final Set<EventId> sagaSources;
  private final AtomicReference<EventId> prevEvent;

  /**
   * The Ids cache (sagaSources and processedCommands) size of 1Million ~= 16Megabyte, since UUID is 32bit -> 16byte
   */
  public PartitionPipeline(Domain domain, CommandRepo commandRepo, EventRepo eventRepo, int partition) {
    // Core
    this.domain = domain;

    // Infra
    this.commandRepo = commandRepo;
    this.eventRepo = eventRepo;
    this.partition = partition;

    // In memory
    this.aggregates = new HashMap<>();
    this.processedCommands = new HashSet<>();
    this.sagaSources = new HashSet<>();
    this.prevEvent = new AtomicReference<>();
  }

  public Flux<Event> handle(Flux<Command> commands) {
    var handleCommands = commands.concatMap(this::decide)
                                 .doOnNext(this::evolve)
                                 .concatMap(this::saga)
                                 .concatMap(eventRepo::publish);
    return initialize().concatWith(handleCommands);
  }

  /**
   * Load previous events and build the state
   */
  Flux<Event> initialize() {
    return this.eventRepo.last(partition)
                         .map(Event::meta)
                         .map(EventMeta::eventId)
                         .flatMapMany(eId -> eventRepo.subUntil(partition, eId))
                         .doOnNext(this::evolve);
  }

  Mono<Event> decide(Command cmd) {
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

  Mono<Event> saga(Event e) {
    return Mono.defer(() -> {
      var opt = domain.saga().apply(e);
      if (opt.isDefined()) {
        return commandRepo.publish(opt.get()).map(_ -> e);
      } else {
        return Mono.just(e);
      }
    });
  }

  void evolve(Event event) {
    event.meta().sagaSource().forEach(sagaSources::add);
    if (aggregates.containsKey(event.meta().stateId())) {
      State currentState = aggregates.get(event.meta().stateId());
      if (hasExpectedVersion(event, currentState)) {
        update(event, domain.evolver().apply(currentState, event));
      } else if (isDuplicate(event)) {
        String message = "Redelivered event[%s], ignoring...".formatted(event.meta());
        log.debug(message);
      } else {
        throw InvalidEvolution.of(event, currentState);
      }
    } else if (event.meta().version() == 0) {
      update(event, domain.evolver().apply(event));
    } else {
      throw InvalidEvolution.of(event);
    }
  }

  boolean hasExpectedVersion(Event event, State currentState) {
    return event.meta().version() == currentState.meta().version() + 1;
  }

  boolean isDuplicate(Event e) {
    return prevEvent.get() == null && prevEvent.get().equals(e.meta().eventId());
  }

  void update(Event e, State newState) {
    aggregates.put(e.meta().stateId(), newState);
    processedCommands.add(e.meta().commandId());
    prevEvent.set(e.meta().eventId());
  }

  boolean isHandledCommand(Command cmd) {
    return processedCommands.contains(cmd.meta().commandId());
  }

  boolean isHandledSagaCommand(Command cmd) {
    return cmd.meta().sagaSource().map(sagaSources::contains).getOrElse(false);
  }
}