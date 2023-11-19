package io.memoria.reactive.testsuite.command;

import io.memoria.atom.eventsourcing.CommandMeta;

public record CreateAccount(CommandMeta meta, String accountName, long balance) implements AccountCommand {}
