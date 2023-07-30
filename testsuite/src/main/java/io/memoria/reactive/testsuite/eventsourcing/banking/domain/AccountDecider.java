package io.memoria.reactive.testsuite.eventsourcing.banking.domain;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.exception.ESException;
import io.memoria.reactive.eventsourcing.rule.Decider;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.ChangeName;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.CloseAccount;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.ConfirmDebit;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.CreateAccount;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.Credit;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.Debit;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountClosed;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountCreated;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.ClosureRejected;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.CreditRejected;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.Credited;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.DebitConfirmed;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.Debited;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.NameChanged;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.ClosedAccount;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.OpenAccount;
import io.vavr.control.Try;

import java.util.function.Supplier;

public record AccountDecider(Supplier<Id> idSupplier, Supplier<Long> timeSupplier)
        implements Decider<Account, AccountCommand, AccountEvent> {

  @Override
  @SuppressWarnings("SwitchStatementWithTooFewBranches")
  public Try<AccountEvent> apply(AccountCommand accountCommand) {
    return switch (accountCommand) {
      case CreateAccount cmd -> Try.success(AccountCreated.from(idSupplier.get(), timeSupplier.get(), cmd));
      default -> Try.failure(ESException.InvalidCommand.of(accountCommand));
    };
  }

  @Override
  public Try<AccountEvent> apply(Account state, AccountCommand command) {
    return switch (state) {
      case OpenAccount openAccount -> handle(openAccount, command);
      case ClosedAccount acc -> handle(acc, command);
    };
  }

  private Try<AccountEvent> handle(OpenAccount state, AccountCommand command) {
    return switch (command) {
      case CreateAccount cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
      case ChangeName cmd -> Try.success(NameChanged.from(idSupplier.get(), timeSupplier.get(), cmd));
      case Debit cmd -> Try.success(Debited.from(idSupplier.get(), timeSupplier.get(), cmd));
      case Credit cmd -> Try.success(Credited.from(idSupplier.get(), timeSupplier.get(), cmd));
      case ConfirmDebit cmd -> Try.success(DebitConfirmed.from(idSupplier.get(), timeSupplier.get(), cmd));
      case CloseAccount cmd -> tryToClose(state, cmd);
    };
  }

  private Try<AccountEvent> handle(ClosedAccount state, AccountCommand command) {
    return switch (command) {
      case Credit cmd -> Try.success(CreditRejected.from(idSupplier.get(), timeSupplier.get(), cmd));
      case ConfirmDebit cmd -> Try.success(DebitConfirmed.from(idSupplier.get(), timeSupplier.get(), cmd));
      case ChangeName cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
      case Debit cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
      case CreateAccount cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
      case CloseAccount cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
    };
  }

  private Try<AccountEvent> tryToClose(OpenAccount openAccount, CloseAccount cmd) {
    if (openAccount.hasOngoingDebit())
      return Try.success(ClosureRejected.from(idSupplier.get(), timeSupplier.get(), cmd));
    return Try.success(AccountClosed.from(idSupplier.get(), timeSupplier.get(), cmd));
  }
}
