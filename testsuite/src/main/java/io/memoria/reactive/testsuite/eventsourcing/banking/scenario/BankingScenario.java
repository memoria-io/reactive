package io.memoria.reactive.testsuite.eventsourcing.banking.scenario;

import io.memoria.reactive.testsuite.eventsourcing.ESScenario;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.command.AccountCommand;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.event.AccountEvent;
import io.memoria.reactive.testsuite.eventsourcing.banking.domain.state.Account;

public interface BankingScenario extends ESScenario<AccountCommand, AccountEvent, Account> {}
