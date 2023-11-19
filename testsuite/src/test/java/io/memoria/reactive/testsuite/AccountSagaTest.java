package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.EventMeta;
import io.memoria.reactive.testsuite.command.Credit;
import io.memoria.reactive.testsuite.event.Debited;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.memoria.reactive.testsuite.TestUtils.aliceId;
import static io.memoria.reactive.testsuite.TestUtils.bobId;
import static io.memoria.reactive.testsuite.TestUtils.saga;

class AccountSagaTest {
  @Test
  void evolve() {
    // Given
    var debited = new Debited(new EventMeta(CommandId.of(), 1, aliceId), bobId, 300);

    // When
    var credit = (Credit) saga.apply(debited).get();

    // Then
    Assertions.assertThat(credit.accountId()).isEqualTo(bobId);
    Assertions.assertThat(credit.debitedAcc()).isEqualTo(aliceId);
  }
}
