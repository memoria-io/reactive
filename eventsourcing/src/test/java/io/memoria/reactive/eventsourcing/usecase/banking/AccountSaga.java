package io.memoria.reactive.eventsourcing.usecase.banking;

import io.memoria.reactive.eventsourcing.rule.Saga;
import io.memoria.reactive.eventsourcing.usecase.banking.command.AccountCommand;
import io.memoria.reactive.eventsourcing.usecase.banking.command.ConfirmDebit;
import io.memoria.reactive.eventsourcing.usecase.banking.command.Credit;
import io.memoria.reactive.eventsourcing.usecase.banking.event.AccountEvent;
import io.memoria.reactive.eventsourcing.usecase.banking.event.CreditRejected;
import io.memoria.reactive.eventsourcing.usecase.banking.event.Credited;
import io.memoria.reactive.eventsourcing.usecase.banking.event.Debited;
import io.vavr.control.Option;

public record AccountSaga() implements Saga<AccountEvent, AccountCommand> {

  @Override
  public Option<AccountCommand> apply(AccountEvent accountEvent) {
    return switch (accountEvent) {
      case Debited e -> Option.some(Credit.of(e.creditedAcc(), e.debitedAcc(), e.amount()));
      case Credited e -> Option.some(ConfirmDebit.of(e.debitedAcc()));
      case CreditRejected e -> Option.some(Credit.of(e.debitedAcc(), e.creditedAcc(), e.amount()));
      default -> Option.none();
    };
  }
}
