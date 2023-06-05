package io.memoria.reactive.eventsourcing.usecase.banking.state;

import io.memoria.atom.core.id.Id;

public record OpenAccount(Id accountId, String name, int balance, int debitCount) implements Account {

  public boolean hasOngoingDebit() {
    return debitCount != 0;
  }

  public OpenAccount withCredit(int credit) {
    return new OpenAccount(accountId, name, balance + credit, debitCount);
  }

  public OpenAccount withDebit(int debit) {
    return new OpenAccount(accountId, name, balance - debit, debitCount + 1);
  }

  public OpenAccount withDebitConfirmed() {
    return new OpenAccount(accountId, name, balance, debitCount - 1);
  }

  public OpenAccount withDebitRejected(int returnedDebit) {
    return new OpenAccount(accountId, name, balance + returnedDebit, debitCount - 1);
  }

  public OpenAccount withName(String newName) {
    return new OpenAccount(accountId, newName, balance, debitCount);
  }
}
