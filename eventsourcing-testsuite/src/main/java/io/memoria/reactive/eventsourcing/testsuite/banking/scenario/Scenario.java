package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import reactor.core.publisher.Mono;

public interface Scenario {
  Mono<Boolean> verify();
}
