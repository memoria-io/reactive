package io.memoria.reactive.testsuite.eventsourcing.banking;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.EventId;
import io.memoria.reactive.eventsourcing.StateId;
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

  public CreateAccount createAccountCmd(StateId stateId, long balance) {
    return new CreateAccount(CommandId.of(idSupplier.get()),
                             stateId,
                             timeSupplier.get(),
                             stateId.id().value(),
                             balance);
  }

  public Flux<AccountCommand> createAccountCmd(Flux<StateId> stateIds, long balance) {
    return stateIds.map(id -> createAccountCmd(id, balance));
  }

  public Flux<AccountCommand> changeNameCmd(Flux<StateId> stateIds, int version) {
    return stateIds.map(stateId -> new ChangeName(CommandId.of(idSupplier.get()),
                                                  stateId,
                                                  timeSupplier.get(),
                                                  String.valueOf(version)));
  }

  public Debit debitCmd(StateId debited, StateId credited, int amount) {
    return new Debit(CommandId.of(idSupplier.get()), debited, timeSupplier.get(), credited, amount);
  }

  public Flux<AccountCommand> debitCmd(Flux<Tuple2<StateId, StateId>> debitedCredited, int amount) {
    return debitedCredited.map(entry -> debitCmd(entry.getT1(), entry.getT2(), amount));
  }

  public CloseAccount closeAccountCmd(StateId stateId) {
    return new CloseAccount(CommandId.of(idSupplier.get()), stateId, timeSupplier.get());
  }

  public Flux<AccountCommand> closeAccounts(Flux<StateId> stateIds) {
    return stateIds.map(this::closeAccountCmd);
  }

  public Flux<AccountEvent> createAccountEvent(Flux<StateId> stateIds, long balance) {
    return stateIds.map(stateId -> createAccountEvent(stateId, balance));
  }

  public AccountEvent createAccountEvent(StateId stateId, long balance) {
    return new AccountCreated(EventId.of(idSupplier.get()),
                              CommandId.of(idSupplier.get()),
                              stateId,
                              timeSupplier.get(),
                              stateId.id().value(),
                              balance);
  }
}
