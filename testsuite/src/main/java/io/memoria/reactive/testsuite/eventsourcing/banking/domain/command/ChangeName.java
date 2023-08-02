package io.memoria.reactive.testsuite.eventsourcing.banking.domain.command;

import io.memoria.atom.eventsourcing.CommandId;
import io.memoria.atom.eventsourcing.StateId;

public record ChangeName(CommandId commandId, StateId accountId, long timestamp, String name)
        implements AccountCommand {}
