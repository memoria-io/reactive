package io.memoria.reactive.eventsourcing.usecase.banking;

import io.memoria.atom.core.id.Id;
import io.memoria.reactive.eventsourcing.usecase.banking.command.AccountCommand;
import io.memoria.reactive.eventsourcing.usecase.banking.command.ChangeName;
import io.memoria.reactive.eventsourcing.usecase.banking.command.CloseAccount;
import io.memoria.reactive.eventsourcing.usecase.banking.command.CreateAccount;
import io.memoria.reactive.eventsourcing.usecase.banking.command.Credit;
import io.memoria.reactive.eventsourcing.usecase.banking.command.Debit;
import io.vavr.collection.List;

import java.util.Random;

class Data {
  public static final String NAME_PREFIX = "name_version:";

  static Id createAccountId(int i) {
    return Id.of("acc_id_" + i);
  }

  static String createName(int nameVersion) {
    return NAME_PREFIX + nameVersion;
  }

  static List<AccountCommand> createAccounts(int nAccounts, int balance) {
    return List.range(0, nAccounts).map(i -> CreateAccount.of(createAccountId(i), createName(i), balance));
  }

  static List<AccountCommand> randomClosure(int nAccounts) {
    return shuffledIds(nAccounts).map(CloseAccount::of);
  }

  static List<AccountCommand> randomOutBounds(int nAccounts, int maxAmount) {
    var accounts = shuffledIds(nAccounts);
    var from = accounts.subSequence(0, nAccounts / 2);
    var to = accounts.subSequence(nAccounts / 2, nAccounts);
    var amounts = List.ofAll(new Random().ints(nAccounts, 1, maxAmount).boxed());
    return List.range(0, nAccounts / 2).map(i -> createOutboundBalance(from.get(i), to.get(i), amounts.get(i)));
  }

  public static List<AccountCommand> changeName(int nAccounts, int version) {
    return List.range(0, nAccounts).map(i -> new ChangeName(createAccountId(i), Id.of(), createName(version)));
  }

  public static List<AccountCommand> credit(int nAccounts, int balance) {
    return List.range(0, nAccounts)
               .map(i -> new Credit(Id.of(), createAccountId(i), Id.of("SomeFakeDebitId"), balance));
  }

  /**
   * Send money from first half to second half of accounts
   */
  public static List<AccountCommand> debit(int nAccounts, int balance) {
    int maxDebitIds = nAccounts / 2;
    return List.range(0, maxDebitIds)
               .map(i -> new Debit(Id.of(), createAccountId(i), createAccountId(nAccounts - i - 1), balance));
  }

  private static AccountCommand createOutboundBalance(Id from, Id to, int amount) {
    return Debit.of(from, to, amount);
  }

  private static List<Id> shuffledIds(int nAccounts) {
    return List.range(0, nAccounts).shuffle().map(Data::createAccountId);
  }

  private Data() {}
}
