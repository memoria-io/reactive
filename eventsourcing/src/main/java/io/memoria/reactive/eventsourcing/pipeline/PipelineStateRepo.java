package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Event;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;

public interface PipelineStateRepo<E extends Event> {
  PipelineRoute route();

  Mono<E> addHandledEvent(E e);

  Mono<Boolean> containsEventId(Id id);

  Mono<Boolean> containsCommandId(Id id);

  static <E extends Event> PipelineStateRepo<E> inMemory(PipelineRoute route) {
    return new MemPipelineStateRepo<>(route);
  }

  static <E extends Event> PipelineStateRepo<E> inMemory(PipelineRoute route, Map<String, Set<Id>> db) {
    return new MemPipelineStateRepo<>(route, db);
  }
}
