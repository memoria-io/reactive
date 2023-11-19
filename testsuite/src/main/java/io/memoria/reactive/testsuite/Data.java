package io.memoria.reactive.testsuite;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.CommandId;
import io.memoria.reactive.eventsourcing.CommandMeta;
import io.memoria.reactive.eventsourcing.StateId;
import io.memoria.reactive.testsuite.command.AccountCommand;
import io.memoria.reactive.testsuite.command.ChangeName;
import io.memoria.reactive.testsuite.command.CloseAccount;
import io.memoria.reactive.testsuite.command.CreateAccount;
import io.memoria.reactive.testsuite.command.Debit;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Data {
  private final AtomicLong counter = new AtomicLong();
  public final Supplier<Id> idSupplier;
  public final Supplier<Long> timeSupplier;

  Data() {
    this.idSupplier = () -> Id.of(counter.getAndIncrement());
    this.timeSupplier = System::currentTimeMillis;
  }

  Data(Supplier<Id> idSupplier, Supplier<Long> timeSupplier) {
    this.idSupplier = idSupplier;
    this.timeSupplier = timeSupplier;
  }

  public static Data ofSerial() {
    return new Data();
  }

  public static Data ofUUID() {
    return new Data(Id::of, System::currentTimeMillis);
  }

  public Id createId(int i) {
    return Id.of(i);
  }

  public Flux<Id> createIds(int from, int to) {
    return Flux.range(from, to).map(this::createId);
  }

  public CreateAccount createAccountCmd(StateId stateId, long balance) {
    return new CreateAccount(createCommandMeta(stateId), stateId.value(), balance);
  }

  public Flux<AccountCommand> createAccountCmd(Flux<StateId> stateIds, long balance) {
    return stateIds.map(id -> createAccountCmd(id, balance));
  }

  public Flux<AccountCommand> changeNameCmd(Flux<StateId> stateIds, int version) {
    return stateIds.map(stateId -> new ChangeName(createCommandMeta(stateId), String.valueOf(version)));
  }

  public Debit debitCmd(StateId debited, StateId credited, int amount) {
    return new Debit(createCommandMeta(debited), credited, amount);
  }

  public Flux<AccountCommand> debitCmd(Flux<Tuple2<StateId, StateId>> debitedCredited, int amount) {
    return debitedCredited.map(entry -> debitCmd(entry.getT1(), entry.getT2(), amount));
  }

  public CloseAccount closeAccountCmd(StateId stateId) {
    return new CloseAccount(createCommandMeta(stateId));
  }

  public Flux<AccountCommand> closeAccounts(Flux<StateId> stateIds) {
    return stateIds.map(this::closeAccountCmd);
  }

  private CommandMeta createCommandMeta(StateId stateId) {
    return new CommandMeta(CommandId.of(idSupplier.get()), stateId, timeSupplier.get());
  }
}
