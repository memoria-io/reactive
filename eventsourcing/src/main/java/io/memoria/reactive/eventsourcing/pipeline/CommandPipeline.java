package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.repo.KVStore;
import io.memoria.reactive.core.stream.ESMsgStream;
import io.memoria.reactive.core.vavr.ReactorVavrUtils;
import io.memoria.reactive.eventsourcing.*;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.vavr.control.Option;
import io.vavr.control.Try;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Function;

public class CommandPipeline<S extends State, C extends Command, E extends Event> {
  // Core
  public final Domain<S, C, E> domain;
  public final PipelineRoute route;
  // Infra
  private final CommandStream<C> commandStream;
  private final EventStream<E> eventStream;
  private final KVStore kvStore;
  private final String kvStoreKey;
  // In memory
  private final Set<Id> processedCommands;
  private final Set<Id> processedEvents;
  private final Map<Id, S> aggregates;

  public CommandPipeline(Domain<S, C, E> domain,
                         PipelineRoute route,
                         ESMsgStream esMsgStream,
                         KVStore kvStore,
                         TextTransformer transformer) {
    this(domain, route, esMsgStream, kvStore, CommandPipeline.class.getSimpleName(), transformer);
  }

  public CommandPipeline(Domain<S, C, E> domain,
                         PipelineRoute route,
                         ESMsgStream esMsgStream,
                         KVStore kvStore,
                         String kvStoreKeyPrefix,
                         TextTransformer transformer) {
    // Core
    this.domain = domain;
    this.route = route;

    // Infra
    this.commandStream = CommandStream.create(esMsgStream, transformer, domain.cClass());
    this.eventStream = EventStream.create(esMsgStream, transformer, domain.eClass());
    this.kvStore = kvStore;
    this.kvStoreKey = kvStoreKeyPrefix + route.eventTopic() + route.eventSubPubPartition();

    // In memory
    this.processedCommands = new HashSet<>();
    this.processedEvents = new HashSet<>();
    this.aggregates = new HashMap<>();
  }

  public Flux<E> handle() {
    return handle(commandStream.sub(route.cmdTopic(), route.cmdSubPartition()));
  }

  public Flux<E> handle(Flux<C> cmds) {
    var handleCommands = cmds.flatMap(this::redirectIfNotBelong) // Redirection allows location transparency and auto sharding
                             .filter(Option::isDefined)
                             .map(Option::get)
                             .map(this::handle)
                             .filter(Option::isDefined)
                             .map(Option::get)
                             .flatMap(t -> ReactorVavrUtils.tryToMono(() -> t))
                             .skipWhile(e -> processedEvents.contains(e.eventId()))
                             .map(this::evolve) // evolve in memory
                             .flatMap(this::storeLastEventId) // then store latest eventId even if possibly not persisted
                             .flatMap(this::pubEvent) // publish event
                             .flatMap(this::saga); // publish a command based on such event
    return init().concatWith(handleCommands);
  }

  /**
   * Load previous events and build the state
   */
  private Flux<E> init() {
    return kvStore.get(this.kvStoreKey)
                  .map(Id::of)
                  .flatMapMany(id -> eventStream.subUntil(route.eventTopic(), route.eventSubPubPartition(), id))
                  .map(this::evolve);
  }

  public Flux<E> subToEvents() {
    return this.eventStream.sub(route.eventTopic(), route.eventSubPubPartition());
  }

  public Flux<C> subToCommands() {
    return this.commandStream.sub(route.cmdTopic(), route.cmdSubPartition());
  }

  public Mono<C> pubCommand(C cmd) {
    return Mono.fromCallable(() -> cmd.partition(route.cmdTotalPubPartitions()))
               .flatMap(partition -> this.commandStream.pub(route.cmdTopic(), partition, cmd));
  }

  public Mono<E> pubEvent(E e) {
    return this.eventStream.pub(route.eventTopic(), route.eventSubPubPartition(), e);
  }

  Mono<Option<C>> redirectIfNotBelong(C cmd) {
    return Mono.fromCallable(() -> {
      if (cmd.isInPartition(route.cmdSubPartition(), route.cmdTotalPubPartitions())) {
        return Mono.just(Option.some(cmd));
      } else {
        return this.pubCommand(cmd).map(c -> Option.<C>none());
      }
    }).flatMap(Function.identity());
  }

  Mono<E> storeLastEventId(E e) {
    return kvStore.set(this.kvStoreKey, e.eventId().value()).map(k -> e);
  }

  Option<Try<E>> handle(C cmd) {
    if (processedCommands.contains(cmd.commandId())) {
      return Option.none();
    }
    if (aggregates.containsKey(cmd.stateId())) {
      return Option.some(domain.decider().apply(aggregates.get(cmd.stateId()), cmd));
    } else {
      return Option.some(domain.decider().apply(cmd));
    }
  }

  Mono<E> saga(E e) {
    return Mono.fromCallable(() -> {
      var sagaCmd = domain.saga().apply(e);
      if (sagaCmd.isDefined() && !this.processedCommands.contains(sagaCmd.get().commandId())) {
        C cmd = sagaCmd.get();
        return this.pubCommand(cmd).map(c -> e);
      } else {
        return Mono.just(e);
      }
    }).flatMap(Function.identity());
  }

  E evolve(E e) {
    S newState;
    if (aggregates.containsKey(e.stateId())) {
      newState = domain.evolver().apply(aggregates.get(e.stateId()), e);
    } else {
      newState = domain.evolver().apply(e);
    }
    aggregates.put(e.stateId(), newState);
    this.processedCommands.add(e.commandId());
    this.processedEvents.add(e.eventId());
    return e;
  }
}