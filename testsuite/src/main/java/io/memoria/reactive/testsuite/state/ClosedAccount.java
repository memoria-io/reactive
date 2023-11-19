package io.memoria.reactive.testsuite.state;

import io.memoria.atom.eventsourcing.StateMeta;

public record ClosedAccount(StateMeta meta) implements Account {}
