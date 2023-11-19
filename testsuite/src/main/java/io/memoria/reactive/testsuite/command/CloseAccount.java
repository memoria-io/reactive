package io.memoria.reactive.testsuite.command;

import io.memoria.reactive.eventsourcing.CommandMeta;

public record CloseAccount(CommandMeta meta) implements AccountCommand {}
