package io.memoria.reactive.testsuite;

import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.EventMeta;
import io.memoria.reactive.eventsourcing.StateMeta;
import io.memoria.reactive.testsuite.event.Debited;
import io.memoria.reactive.testsuite.state.OpenAccount;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.memoria.reactive.testsuite.TestUtils.alice;
import static io.memoria.reactive.testsuite.TestUtils.aliceId;
import static io.memoria.reactive.testsuite.TestUtils.bobId;
import static io.memoria.reactive.testsuite.TestUtils.evolver;

class AccountEvolverTest {
  @Test
  void evolve() {
    // Given
    var openAccount = new OpenAccount(new StateMeta(aliceId), alice, 500);
    var debited = new Debited(new EventMeta(CommandId.of(), 1, aliceId), bobId, 300);

    // When
    var acc = (OpenAccount) evolver.apply(openAccount, debited);

    // Then
    Assertions.assertThat(acc.balance()).isEqualTo(200);
  }
}
