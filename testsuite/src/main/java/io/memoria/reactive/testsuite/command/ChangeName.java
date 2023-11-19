package io.memoria.reactive.testsuite.command;

import io.memoria.reactive.eventsourcing.CommandMeta;

public record ChangeName(CommandMeta meta, String name) implements AccountCommand {}
