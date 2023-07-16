package io.memoria.reactive.eventsourcing.testsuite.banking.domain;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.exception.ESException.InvalidEvent;
import io.memoria.reactive.eventsourcing.rule.Evolver;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountClosed;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountCreated;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.Credited;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.DebitConfirmed;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.Debited;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.NameChanged;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.Account;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.ClosedAccount;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.OpenAccount;

import java.util.function.Supplier;

@SuppressWarnings("SwitchStatementWithTooFewBranches")
public record AccountEvolver(Supplier<Id> idSupplier, Supplier<Long> timeSupplier)
        implements Evolver<Account, AccountEvent> {
  @Override
  public Account apply(Account account, AccountEvent accountEvent) {
    return switch (account) {
      case OpenAccount openAccount -> handle(openAccount, accountEvent);
      case ClosedAccount acc -> acc;
    };
  }

  @Override
  public Account apply(AccountEvent accountEvent) {
    return switch (accountEvent) {
      case AccountCreated e -> new OpenAccount(e.stateId(), e.name(), e.balance(), 0);
      default -> throw InvalidEvent.of(accountEvent);
    };
  }

  private Account handle(OpenAccount openAccount, AccountEvent accountEvent) {
    return switch (accountEvent) {
      case Credited e -> openAccount.withCredit(e.amount());
      case NameChanged e -> openAccount.withName(e.newName());
      case Debited e -> openAccount.withDebit(e.amount());
      case DebitConfirmed e -> openAccount.withDebitConfirmed();
      case AccountClosed e -> new ClosedAccount(e.accountId());
      default -> openAccount;
    };
  }
}
