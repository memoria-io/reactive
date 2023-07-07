package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.core.text.TextTransformer;
import io.memoria.reactive.core.stream.ESMsgStream;
import io.memoria.reactive.core.vavr.ReactorVavrUtils;
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
import java.util.function.Function;

public class CommandPipeline<S extends State, C extends Command, E extends Event> {
  // Core
  public final Domain<S, C, E> domain;
  public final PipelineRoute route;
  // Infra
  private final CommandStream<C> commandStream;
  private final EventStream<E> eventStream;
  // In memory
  private final PipelineStateRepo pipelineState;
  private final Map<Id, S> aggregates;

  public CommandPipeline(Domain<S, C, E> domain,
                         PipelineRoute route,
                         ESMsgStream esMsgStream,
                         PipelineStateRepo pipelineState,
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
    var handleCommands = cmds.flatMap(this::redirectIfNotBelong) // Redirection allows location transparency and auto sharding
                             .concatMap(this::handle) // handle the command
                             .flatMap(this::evolve) // evolve in memory
                             .flatMap(this::pubEvent) // publish event
                             .flatMap(this::saga); // publish a command based on such event
    return init().concatWith(handleCommands);
  }

  /**
   * Load previous events and build the state
   */
  private Flux<E> init() {
    return this.pipelineState.lastEventId()
                             .flatMapMany(id -> eventStream.subUntil(route.eventTopic(),
                                                                     route.eventSubPubPartition(),
                                                                     id))
                             .flatMap(this::evolve);
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

  Mono<C> redirectIfNotBelong(C cmd) {
    return Mono.fromCallable(() -> {
      if (cmd.isInPartition(route.cmdSubPartition(), route.cmdTotalPubPartitions())) {
        return Mono.<C>empty();
      } else {
        return this.pubCommand(cmd);
      }
    }).flatMap(Function.identity());
  }

  Mono<E> handle(C cmd) {
    return this.pipelineState.containsCommandId(cmd.commandId()).flatMap(exists -> {
      if (exists) {
        return Mono.empty();
      } else {
        if (aggregates.containsKey(cmd.stateId())) {
          return ReactorVavrUtils.tryToMono(() -> domain.decider().apply(aggregates.get(cmd.stateId()), cmd));
        } else {
          return ReactorVavrUtils.tryToMono(() -> domain.decider().apply(cmd));
        }
      }
    });
  }

  Mono<E> saga(E e) {
    return Mono.fromCallable(() -> {
      var sagaCmd = domain.saga().apply(e);
      if (sagaCmd.isDefined() && !this.pipelineState.contains(sagaCmd.get().commandId())) {
        C cmd = sagaCmd.get();
        return this.pubCommand(cmd).map(c -> e);
      } else {
        return Mono.just(e);
      }
    }).flatMap(Function.identity());
  }

  Mono<E> evolve(E e) {
    return pipelineState.containsEventId(e.eventId()).flatMap(exists -> {
      if (exists) {
        return Mono.empty();
      } else {
        S newState;
        if (aggregates.containsKey(e.stateId())) {
          newState = domain.evolver().apply(aggregates.get(e.stateId()), e);
        } else {
          newState = domain.evolver().apply(e);
        }
        aggregates.put(e.stateId(), newState);
        this.pipelineState.addCommandId(e.commandId());
        this.pipelineState.addEventId(e.eventId());
        return Mono.just(e);
      }
    });
  }
}