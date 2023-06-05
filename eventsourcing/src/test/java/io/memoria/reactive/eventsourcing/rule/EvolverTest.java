package io.memoria.reactive.eventsourcing.rule;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.usecase.banking.AccountEvolver;
import io.memoria.reactive.eventsourcing.usecase.banking.event.*;
import io.memoria.reactive.eventsourcing.usecase.banking.state.OpenAccount;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class EvolverTest {
  private static final Id id = Id.of(0);
  private static final Id bobId = Id.of("bob");
  private static final AccountCreated accCreated = new AccountCreated(id, id, bobId, "bob0", 0);
  private static final NameChanged nameChanged1 = new NameChanged(id, id, bobId, "bob1");
  private static final NameChanged nameChanged2 = new NameChanged(id, id, bobId, "bob2");
  private static final Credited credited = new Credited(id, id, bobId, Id.of("jan"), 10);

  @Test
  void reduce() {
    // When
    var events = Flux.<AccountEvent>just(accCreated, nameChanged1, nameChanged2, credited);
    var evolver = new AccountEvolver();

    // Then
    StepVerifier.create(evolver.reduce(bobId, events))
                .expectNext(new OpenAccount(bobId, "bob2", 10, 0))
                .verifyComplete();
  }

  @Test
  void accumulate() {
    // When
    var events = Flux.<AccountEvent>just(accCreated, nameChanged1, nameChanged2, credited);
    var evolver = new AccountEvolver();

    // Then
    StepVerifier.create(evolver.allStates(bobId, events))
                .expectNext(new OpenAccount(bobId, "bob0", 0, 0))
                .expectNext(new OpenAccount(bobId, "bob1", 0, 0))
                .expectNext(new OpenAccount(bobId, "bob2", 0, 0))
                .expectNext(new OpenAccount(bobId, "bob2", 10, 0))
                .verifyComplete();
  }
}
