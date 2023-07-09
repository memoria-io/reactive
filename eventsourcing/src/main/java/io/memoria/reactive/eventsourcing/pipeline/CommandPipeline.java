package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.reactor.ReactorUtils;
import io.memoria.reactive.core.stream.ESMsgStream;
import io.memoria.reactive.eventsourcing.Command;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.Event;
import io.memoria.reactive.eventsourcing.State;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

import static io.memoria.reactive.core.reactor.ReactorUtils.booleanToMono;

public class CommandPipeline<S extends State, C extends Command, E extends Event> {
  // Core
  public final Domain<S, C, E> domain;
  public final PipelineRoute route;
  // Infra
  private final CommandStream<C> commandStream;
  private final EventStream<E> eventStream;
  // In memory
  private final PipelineStateRepo<E> pipelineState;
  private final Map<Id, S> aggregates;

  public CommandPipeline(Domain<S, C, E> domain,
                         PipelineRoute route,
                         ESMsgStream esMsgStream,
                         PipelineStateRepo<E> pipelineState,
                         TextTransformer transformer) {
    // Core
    this.domain = domain;
    this.route = route;

    // Infra
    this.commandStream = CommandStream.create(esMsgStream, transformer, domain.cClass());
    this.eventStream = EventStream.create(esMsgStream, transformer, domain.eClass());

    // In memory
    this.pipelineState = pipelineState;
    this.aggregates = new HashMap<>();
  }

  public Flux<E> handle() {
    return handle(commandStream.sub(route.cmdTopic(), route.cmdSubPartition()));
  }

  public Flux<E> handle(Flux<C> cmds) {
    var handleCommands = cmds.concatMap(this::redirectIfNotBelong) // Redirection allows location transparency and auto sharding
                             .concatMap(this::handleCommand) // handle the command
                             .concatMap(this::evolve) // evolve the state
                             .concatMap(this::pubEvent) // publish the event
                             .concatMap(this::saga); // publish saga command
    return init().concatWith(handleCommands);
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

  /**
   * Load previous events and build the state
   */
  Flux<E> init() {
    return this.pipelineState.getLastEventId().flatMapMany(this::subUntil).concatMap(this::evolve);
  }

  Flux<E> subUntil(Id id) {
    return eventStream.subUntil(route.eventTopic(), route.eventSubPubPartition(), id);
  }

  Mono<C> redirectIfNotBelong(C cmd) {
    if (cmd.isInPartition(route.cmdSubPartition(), route.cmdTotalPubPartitions())) {
      return Mono.just(cmd);
    } else {
      return this.pubCommand(cmd).flatMap(c -> Mono.empty());
    }
  }

  Mono<E> handleCommand(C cmd) {
    return this.pipelineState.containsCommandId(cmd.commandId())
                             .flatMap(exists -> booleanToMono(!exists, () -> decide(cmd)));
  }

  private Mono<E> decide(C cmd) {
    if (aggregates.containsKey(cmd.stateId())) {
      return ReactorUtils.tryToMono(() -> domain.decider().apply(aggregates.get(cmd.stateId()), cmd));
    } else {
      return ReactorUtils.tryToMono(() -> domain.decider().apply(cmd));
    }
  }

  Mono<E> evolve(E e) {
    return pipelineState.containsEventId(e.eventId()).flatMap(exists -> booleanToMono(!exists, () -> handleEvent(e)));
  }

  Mono<E> saga(E e) {
    return domain.saga().apply(e).map(this::handleSaga).map(mono -> mono.map(c -> e)).getOrElse(Mono.just(e));
  }

  Mono<C> handleSaga(C cmd) {
    return this.pipelineState.containsCommandId(cmd.commandId())
                             .flatMap(exists -> booleanToMono(!exists, () -> this.pubCommand(cmd)));
  }

  Mono<E> handleEvent(E e) {
    S newState;
    if (aggregates.containsKey(e.stateId())) {
      newState = domain.evolver().apply(aggregates.get(e.stateId()), e);
    } else {
      newState = domain.evolver().apply(e);
    }
    aggregates.put(e.stateId(), newState);
    return this.pipelineState.addHandledEvent(e);
  }
}