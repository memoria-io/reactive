package io.memoria.reactive.testsuite.eventsourcing.banking.domain;

import io.memoria.atom.core.id.Id;
import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.rule.Saga;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.ConfirmDebit;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.Credit;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.CreditRejected;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.Credited;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.Debited;
import io.vavr.control.Option;

import java.util.function.Supplier;

public record AccountSaga(Supplier<Id> idSupplier, Supplier<Long> timeSupplier)
        implements Saga<AccountEvent, AccountCommand> {

  @Override
  public Option<AccountCommand> apply(AccountEvent accountEvent) {
    return switch (accountEvent) {
      case Debited e -> Option.some(new Credit(CommandId.of(idSupplier.get()),
                                               e.creditedAcc(),
                                               timeSupplier.get(),
                                               e.debitedAcc(),
                                               e.amount()));
      case Credited e ->
              Option.some(new ConfirmDebit(CommandId.of(idSupplier.get()), e.debitedAcc(), timeSupplier.get()));
      case CreditRejected e -> Option.some(new Credit(CommandId.of(idSupplier.get()),
                                                      e.debitedAcc(),
                                                      timeSupplier.get(),
                                                      e.creditedAcc(),
                                                      e.amount()));
      default -> Option.none();
    };
  }
}
