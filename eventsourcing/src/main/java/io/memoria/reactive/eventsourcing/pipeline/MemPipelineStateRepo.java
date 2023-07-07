package io.memoria.reactive.eventsourcing.pipeline;

import io.memoria.atom.core.id.Id;
import io.vavr.control.Option;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class MemPipelineStateRepo implements PipelineStateRepo {
  private final PipelineRoute route;
  private final Map<String, Set<Id>> db;
  private final Map<String, Id> lastEventId;
  private final String eventKey;
  private final String commandKey;

  public MemPipelineStateRepo(PipelineRoute route) {
    this(route, new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
  }

  public MemPipelineStateRepo(PipelineRoute route, Map<String, Set<Id>> db, Map<String, Id> lastEventId) {
    this.route = route;
    this.db = db;
    this.lastEventId = lastEventId;
    this.eventKey = toKey(route.eventTopic(), route.eventSubPubPartition());
    this.commandKey = toKey(route.cmdTopic(), route.cmdSubPartition());
  }

  @Override
  public PipelineRoute route() {
    return this.route;
  }

  @Override
  public Mono<Boolean> addEventId(Id id) {
    return Mono.fromCallable(() -> add(eventKey, id));
  }

  private boolean add(String key, Id id) {
    db.computeIfPresent(key, (k, v) -> {
      v.add(id);
      return v;
    });

    db.computeIfAbsent(key, k -> {
      var set = new HashSet<Id>();
      set.add(id);
      return set;
    });

    lastEventId.put(key, id);
    return true;
  }

  @Override
  public Mono<Boolean> containsEventId(Id id) {
    return Mono.fromCallable(() -> db.containsKey(eventKey) && db.get(eventKey).contains(id));
  }

  @Override
  public Mono<Boolean> addCommandId(Id id) {
    return Mono.fromCallable(() -> add(commandKey, id));
  }

  @Override
  public Mono<Boolean> containsCommandId(Id id) {
    return Mono.fromCallable(() -> db.containsKey(commandKey) && db.get(commandKey).contains(id));
  }

  @Override
  public Mono<Option<Id>> lastEventId() {
    return Mono.fromCallable(() -> Option.of(this.lastEventId.get(eventKey)));
  }

  private static String toKey(String topic, int partition) {
    return String.format("%s_%d".formatted(topic, partition));
  }
}
