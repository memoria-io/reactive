package io.memoria.reactive.testsuite;

import io.memoria.atom.eventsourcing.ESException.InvalidEvent;
import io.memoria.atom.eventsourcing.StateMeta;
import io.memoria.atom.eventsourcing.rule.Evolver;
import io.memoria.reactive.testsuite.event.AccountClosed;
import io.memoria.reactive.testsuite.event.AccountCreated;
import io.memoria.reactive.testsuite.event.AccountEvent;
import io.memoria.reactive.testsuite.event.Credited;
import io.memoria.reactive.testsuite.event.DebitConfirmed;
import io.memoria.reactive.testsuite.event.Debited;
import io.memoria.reactive.testsuite.event.NameChanged;
import io.memoria.reactive.testsuite.state.Account;
import io.memoria.reactive.testsuite.state.ClosedAccount;
import io.memoria.reactive.testsuite.state.OpenAccount;

@SuppressWarnings("SwitchStatementWithTooFewBranches")
public record AccountEvolver() implements Evolver<Account, AccountEvent> {
  @Override
  public Account apply(AccountEvent accountEvent) {
    return switch (accountEvent) {
      case AccountCreated e -> {
        StateMeta meta = new StateMeta(e.accountId());
        yield new OpenAccount(meta, e.name(), e.balance(), 0, 0, 0);
      }
      default -> throw InvalidEvent.of(accountEvent);
    };
  }

  @Override
  public Account apply(Account account, AccountEvent accountEvent) {
    return switch (account) {
      case OpenAccount openAccount -> handle(openAccount, accountEvent);
      case ClosedAccount acc -> acc;
    };
  }

  private Account handle(OpenAccount account, AccountEvent accountEvent) {
    return switch (accountEvent) {
      case Credited e -> account.withCredit(e.amount());
      case NameChanged e -> account.withName(e.newName());
      case Debited e -> account.withDebit(e.amount());
      case DebitConfirmed e -> account.withDebitConfirmed();
      case AccountClosed e -> new ClosedAccount(stateMeta(account));
      default -> account;
    };
  }
}
