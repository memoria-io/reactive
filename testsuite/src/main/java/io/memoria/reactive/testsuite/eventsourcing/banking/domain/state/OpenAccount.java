package io.memoria.reactive.testsuite.eventsourcing.banking.domain.state;

import io.memoria.atom.eventsourcing.StateId;

public record OpenAccount(StateId accountId,
                          String name,
                          long balance,
                          int debitCount,
                          int confirmedDebit,
                          int creditCount) implements Account {

  public boolean hasOngoingDebit() {
    return debitCount != confirmedDebit;
  }

  public OpenAccount withCredit(long credit) {
    return new OpenAccount(accountId, name, balance + credit, debitCount, confirmedDebit, creditCount + 1);
  }

  public OpenAccount withDebit(long debit) {
    return new OpenAccount(accountId, name, balance - debit, debitCount + 1, confirmedDebit, creditCount);
  }

  public OpenAccount withDebitConfirmed() {
    return new OpenAccount(accountId, name, balance, debitCount, confirmedDebit + 1, creditCount);
  }

  public OpenAccount withDebitRejected(long returnedDebit) {
    return new OpenAccount(accountId, name, balance + returnedDebit, debitCount - 1, confirmedDebit, creditCount);
  }

  public OpenAccount withName(String newName) {
    return new OpenAccount(accountId, newName, balance, debitCount, confirmedDebit, creditCount);
  }
}
