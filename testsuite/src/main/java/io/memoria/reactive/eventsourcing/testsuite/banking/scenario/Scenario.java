package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface Scenario {
  Flux<AccountCommand> publishCommands();

  Flux<AccountEvent> handleCommands();

  Mono<Boolean> verify(Flux<AccountEvent> events);
}
