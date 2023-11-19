package io.memoria.reactive.testsuite;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.rule.Saga;
import io.memoria.reactive.testsuite.command.AccountCommand;
import io.memoria.reactive.testsuite.command.ConfirmDebit;
import io.memoria.reactive.testsuite.command.Credit;
import io.memoria.reactive.testsuite.event.AccountEvent;
import io.memoria.reactive.testsuite.event.CreditRejected;
import io.memoria.reactive.testsuite.event.Credited;
import io.memoria.reactive.testsuite.event.Debited;
import io.vavr.control.Option;

import java.util.function.Supplier;

public record AccountSaga(Supplier<Id> idSupplier, Supplier<Long> timeSupplier)
        implements Saga<AccountEvent, AccountCommand> {

  @Override
  public Option<AccountCommand> apply(AccountEvent event) {
    return switch (event) {
      case Debited e ->
              Option.some(new Credit(commandMeta(e.creditedAcc(), e.meta().eventId()), e.accountId(), e.amount()));
      case Credited e -> Option.some(new ConfirmDebit(commandMeta(e.debitedAcc(), e.meta().eventId()), e.accountId()));
      case CreditRejected e ->
              Option.some(new Credit(commandMeta(e.debitedAcc(), e.meta().eventId()), e.accountId(), e.amount()));
      default -> Option.none();
    };
  }
}
