package io.memoria.reactive.eventsourcing.testsuite.banking.scenario;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.Domain;
import io.memoria.reactive.eventsourcing.pipeline.CommandPipeline;
import io.memoria.reactive.eventsourcing.pipeline.CommandRoute;
import io.memoria.reactive.eventsourcing.pipeline.EventRoute;
import io.memoria.reactive.eventsourcing.stream.CommandStream;
import io.memoria.reactive.eventsourcing.stream.EventStream;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.AccountDecider;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.AccountEvolver;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.AccountSaga;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.AccountCommand;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.ChangeName;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.CloseAccount;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.CreateAccount;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.Credit;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.command.Debit;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.event.AccountEvent;
import io.memoria.reactive.eventsourcing.testsuite.banking.domain.state.Account;
import io.vavr.collection.List;
import io.vavr.collection.Map;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class Data {
  private final String namePrefix;
  private final AtomicLong counter = new AtomicLong();
  private final Supplier<Id> idSupplier;
  private final Supplier<Long> timeSupplier;
  public final Domain<Account, AccountCommand, AccountEvent> domain;

  Data(String namePrefix) {
    this.namePrefix = namePrefix;
    this.idSupplier = () -> Id.of(counter.getAndIncrement());
    this.timeSupplier = System::currentTimeMillis;
    this.domain = stateDomain();
  }

  Data(String namePrefix, Supplier<Id> idSupplier, Supplier<Long> timeSupplier) {
    this.namePrefix = namePrefix;
    this.idSupplier = idSupplier;
    this.timeSupplier = timeSupplier;
    this.domain = stateDomain();
  }

  public static Data ofSerial(String namePrefix) {
    return new Data(namePrefix);
  }

  public static Data ofUUID(String namePrefix) {
    return new Data(namePrefix, Id::of, System::currentTimeMillis);
  }

  public CommandPipeline<Account, AccountCommand, AccountEvent> createMemoryPipeline(CommandRoute commandRoute,
                                                                                     EventRoute eventRoute) {
    return new CommandPipeline<>(stateDomain(),
                                 CommandStream.inMemory(),
                                 commandRoute,
                                 EventStream.inMemory(),
                                 eventRoute);
  }

  public CommandPipeline<Account, AccountCommand, AccountEvent> createPipeline(CommandStream<AccountCommand> commandStream,
                                                                               CommandRoute commandRoute,
                                                                               EventStream<AccountEvent> eventStream,
                                                                               EventRoute eventRoute) {
    return new CommandPipeline<>(stateDomain(), commandStream, commandRoute, eventStream, eventRoute);
  }

  public Id createId(int i) {
    return Id.of(namePrefix + i);
  }

  public List<Id> createIds(int from, int to) {
    return List.range(from, to).map(this::createId);
  }

  public List<Id> createShuffledIds(int nAccounts) {
    return List.range(0, nAccounts).shuffle().map(this::createId);
  }

  public String createName(int nameVersion) {
    return namePrefix + nameVersion;
  }

  public CreateAccount createAccountCmd(Id id, long balance) {
    return new CreateAccount(idSupplier.get(), id, timeSupplier.get(), createName(0), balance);
  }

  public List<AccountCommand> createAccountCmd(List<Id> ids, long balance) {
    return ids.map(id -> createAccountCmd(id, balance));
  }

  public List<AccountCommand> changeNameCmd(List<Id> ids, int version) {
    return ids.map(id -> new ChangeName(idSupplier.get(), id, timeSupplier.get(), createName(version)));
  }

  public Credit creditCmd(Id debited, int balance, Id id) {
    return new Credit(idSupplier.get(), id, timeSupplier.get(), debited, balance);
  }

  public List<AccountCommand> creditCmd(List<Id> credited, Id debited, int balance) {
    return credited.map(id -> creditCmd(debited, balance, id)).map(i -> (AccountCommand) i).toList();
  }

  public List<AccountCommand> creditCmd(Map<Id, Id> creditedDebited, int balance) {
    return creditedDebited.map(entry -> creditCmd(entry._2, balance, entry._1)).map(i -> (AccountCommand) i).toList();
  }

  public Debit debitCmd(Id debited, Id credited, int amount) {
    return new Debit(idSupplier.get(), debited, timeSupplier.get(), credited, amount);
  }

  public List<AccountCommand> debitCmd(Map<Id, Id> debitedCredited, int amount) {
    return debitedCredited.map(entry -> debitCmd(entry._1, entry._2, amount)).map(i -> (AccountCommand) i).toList();
  }

  public List<AccountCommand> randomDebitAmountCmd(Map<Id, Id> debitedCredited, int maxAmount) {
    var random = new Random();
    return debitedCredited.map(entry -> debitCmd(entry._1, entry._2, random.nextInt(maxAmount)))
                          .map(i -> (AccountCommand) i)
                          .toList();
  }

  public CloseAccount closeAccountCmd(Id i) {
    return new CloseAccount(idSupplier.get(), i, timeSupplier.get());
  }

  public List<AccountCommand> toCloseAccountCmd(List<Id> ids) {
    return ids.map(this::closeAccountCmd);
  }

  public String namePrefix() {return namePrefix;}

  public AtomicLong counter() {return counter;}

  public Supplier<Id> idSupplier() {return idSupplier;}

  public Supplier<Long> timeSupplier() {return timeSupplier;}

  private Domain<Account, AccountCommand, AccountEvent> stateDomain() {
    return new Domain<>(Account.class,
                        AccountCommand.class,
                        AccountEvent.class,
                        new AccountDecider(idSupplier, timeSupplier),
                        new AccountEvolver(idSupplier, timeSupplier),
                        new AccountSaga(idSupplier, timeSupplier));
  }
}
