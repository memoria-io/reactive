package io.memoria.reactive.testsuite.eventsourcing.banking;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.ChangeName;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.CloseAccount;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.CreateAccount;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.Debit;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountCreated;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class BankingData {
  private final AtomicLong counter = new AtomicLong();
  public final Supplier<Id> idSupplier;
  public final Supplier<Long> timeSupplier;

  BankingData() {
    this.idSupplier = () -> Id.of(counter.getAndIncrement());
    this.timeSupplier = System::currentTimeMillis;
  }

  BankingData(Supplier<Id> idSupplier, Supplier<Long> timeSupplier) {
    this.idSupplier = idSupplier;
    this.timeSupplier = timeSupplier;
  }

  public static BankingData ofSerial() {
    return new BankingData();
  }

  public static BankingData ofUUID() {
    return new BankingData(Id::of, System::currentTimeMillis);
  }

  public Id createId(int i) {
    return Id.of(i);
  }

  public Flux<Id> createIds(int from, int to) {
    return Flux.range(from, to).map(this::createId);
  }

  public CreateAccount createAccountCmd(Id id, long balance) {
    return new CreateAccount(idSupplier.get(), id, timeSupplier.get(), id.value(), balance);
  }

  public Flux<AccountCommand> createAccountCmd(Flux<Id> ids, long balance) {
    return ids.map(id -> createAccountCmd(id, balance));
  }

  public Flux<AccountCommand> changeNameCmd(Flux<Id> ids, int version) {
    return ids.map(id -> new ChangeName(idSupplier.get(), id, timeSupplier.get(), String.valueOf(version)));
  }

  public Debit debitCmd(Id debited, Id credited, int amount) {
    return new Debit(idSupplier.get(), debited, timeSupplier.get(), credited, amount);
  }

  public Flux<AccountCommand> debitCmd(Flux<Tuple2<Id, Id>> debitedCredited, int amount) {
    return debitedCredited.map(entry -> debitCmd(entry.getT1(), entry.getT2(), amount));
  }

  public CloseAccount closeAccountCmd(Id i) {
    return new CloseAccount(idSupplier.get(), i, timeSupplier.get());
  }

  public Flux<AccountCommand> closeAccounts(Flux<Id> ids) {
    return ids.map(this::closeAccountCmd);
  }

  public Flux<AccountEvent> createAccountEvent(Flux<Id> ids, long balance) {
    return ids.map(id -> createAccountEvent(id, balance));
  }

  public AccountEvent createAccountEvent(Id id, long balance) {
    return new AccountCreated(idSupplier.get(), idSupplier.get(), id, timeSupplier.get(), id.value(), balance);
  }
}
