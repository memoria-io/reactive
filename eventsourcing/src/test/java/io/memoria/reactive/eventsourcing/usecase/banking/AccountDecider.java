package io.memoria.reactive.eventsourcing.usecase.banking;

import io.memoria.reactive.eventsourcing.exception.ESException;
import io.memoria.reactive.eventsourcing.rule.Decider;
import io.memoria.reactive.eventsourcing.usecase.banking.command.*;
import io.memoria.reactive.eventsourcing.usecase.banking.event.*;
import io.memoria.reactive.eventsourcing.usecase.banking.state.Account;
import io.memoria.reactive.eventsourcing.usecase.banking.state.ClosedAccount;
import io.memoria.reactive.eventsourcing.usecase.banking.state.OpenAccount;
import io.vavr.control.Try;

public record AccountDecider() implements Decider<Account, AccountCommand, AccountEvent> {

  @Override
  @SuppressWarnings("SwitchStatementWithTooFewBranches")
  public Try<AccountEvent> apply(AccountCommand accountCommand) {
    return switch (accountCommand) {
      case CreateAccount cmd -> Try.success(AccountCreated.from(cmd));
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
      case ChangeName cmd -> Try.success(NameChanged.from(state, cmd));
      case Debit cmd -> Try.success(Debited.from(state, cmd));
      case Credit cmd -> Try.success(Credited.from(state, cmd));
      case ConfirmDebit cmd -> Try.success(DebitConfirmed.from(state, cmd));
      case CloseAccount cmd -> tryToClose(state, cmd);
    };
  }

  private Try<AccountEvent> handle(ClosedAccount state, AccountCommand command) {
    return switch (command) {
      case Credit cmd -> Try.success(CreditRejected.from(state, cmd));
      case ConfirmDebit cmd -> Try.success(DebitConfirmed.from(state, cmd));
      case ChangeName cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
      case Debit cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
      case CreateAccount cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
      case CloseAccount cmd -> Try.failure(ESException.InvalidCommand.of(state, cmd));
    };
  }

  private Try<AccountEvent> tryToClose(OpenAccount openAccount, CloseAccount cmd) {
    if (openAccount.hasOngoingDebit())
      return Try.success(ClosureRejected.from(openAccount, cmd));
    return Try.success(AccountClosed.from(openAccount, cmd));
  }
}
