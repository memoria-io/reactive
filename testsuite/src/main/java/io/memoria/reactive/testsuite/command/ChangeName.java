package io.memoria.reactive.testsuite.command;

import io.memoria.atom.eventsourcing.CommandMeta;

public record ChangeName(CommandMeta meta, String name) implements AccountCommand {}
