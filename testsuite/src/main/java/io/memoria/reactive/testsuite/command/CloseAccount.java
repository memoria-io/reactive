package io.memoria.reactive.testsuite.command;

import io.memoria.atom.eventsourcing.CommandMeta;

public record CloseAccount(CommandMeta meta) implements AccountCommand {}
