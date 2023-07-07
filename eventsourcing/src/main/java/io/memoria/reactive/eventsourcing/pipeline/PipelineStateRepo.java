package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.id.Id;
import io.vavr.control.Option;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;

public interface PipelineStateRepo {
  PipelineRoute route();

  Mono<Boolean> addEventId(Id id);

  Mono<Boolean> containsEventId(Id id);

  Mono<Boolean> addCommandId(Id id);

  Mono<Boolean> containsCommandId(Id id);

  Mono<Id> lastEventId();

  static PipelineStateRepo inMemory(PipelineRoute route) {
    return new MemPipelineStateRepo(route);
  }

  static PipelineStateRepo inMemory(PipelineRoute route, Map<String, Set<Id>> db, Map<String, Id> lastEvent) {
    return new MemPipelineStateRepo(route, db, lastEvent);
  }
}
