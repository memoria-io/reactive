package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.CommandMeta;
import io.memoria.atom.eventsourcing.StateMeta;
import io.memoria.reactive.testsuite.command.Debit;
import io.memoria.reactive.testsuite.event.DebitRejected;
import io.memoria.reactive.testsuite.event.Debited;
import io.memoria.reactive.testsuite.state.OpenAccount;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.memoria.reactive.testsuite.TestUtils.alice;
import static io.memoria.reactive.testsuite.TestUtils.aliceId;
import static io.memoria.reactive.testsuite.TestUtils.bobId;
import static io.memoria.reactive.testsuite.TestUtils.decider;
import static org.assertj.core.api.Assertions.assertThat;

class AccountDeciderTest {

  @ParameterizedTest
  @ValueSource(ints = {300, 500, 600})
  void debit(int debitAmount) {
    // Given
    int balance = 500;
    var openAccount = new OpenAccount(new StateMeta(aliceId), alice, balance);
    var debit = new Debit(new CommandMeta(aliceId), bobId, debitAmount);

    // When
    var event = decider.apply(openAccount, debit).get();

    // Then
    assertThat(event.accountId()).isEqualTo(aliceId);
    if (debitAmount < balance) {
      assertThat(event).isInstanceOf(Debited.class);
    } else {
      assertThat(event).isInstanceOf(DebitRejected.class);
    }
  }
}
