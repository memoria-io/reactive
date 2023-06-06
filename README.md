# Reactive

[![Release](https://github.com/memoria-io/reactive/workflows/Release/badge.svg)](https://github.com/memoria-io/reactive/actions?query=workflow%3ARelease)
[![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/memoria-io/reactive?label=Version&logo=github)](https://github.com/orgs/memoria-io/packages?repo_name=reactive)

[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=bugs)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=coverage)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=memoria-io_reactive&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=memoria-io_reactive)


> هذا العلم والعمل وقف للّه تعالي اسأل اللّه ان يرزقنا الاخلاص فالقول والعمل
>
> This work and knowledge is for the sake of Allah, may Allah grant us sincerity in what we say or do.

## Introduction

Based on atom library and driven by same motivation, `reactive` is a collection of utilities for web, and event based
applications.

* Reactive relies heavily on [Reactive Streams](https://www.streams.org/) and
  uses [Project-Reactor](https://projectreactor.io/),
  [Reactor Netty](https://github.com/reactor/reactor-netty).

## Modules

### Core module

Core module has basic set of reactive utilities and standard interfaces for reactive streams and event repos

### Eventsourcing module

* Basic Eventsourcing functional interface
* Reactive pipeline
* Usage examples can be found in tests

### `web` module

* Utilities and convenience methods for reactive web and `reactor-netty`

### `kafka`, `nats` modules

Adapter modules initially built to implement eventsourcing infra ports.

## Usage

First make sure you can fetch repositories under such memoria organisation from github

```xml

<repositories>
    <repository>
        <id>github</id>
        <name>GitHub Packages</name>
        <url>https://maven.pkg.github.com/memoria-io/*</url>
        <releases>
            <enabled>true</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>

```

Then import nomrally in your pom dependencies

```xml

<dependency>
    <groupId>io.memoria</groupId>
    <artifactId>reactive</artifactId>
    <version>20.0.0</version>
</dependency>
```

## Versioning

The versioning is similar to semantic but with a shift where the first segment being the jdk version.

Format Example: `JDK_Version.major.mino`

## TODOs

* [x] Event Sourcing
    * [x] State decider, evolver, Stream commandAggregate
    * [x] Sagas decider, Stream commandAggregate
    * [x] id safety with typed classed (StateId, CommandId, EventId)
    * [x] Reducer
        * If using reduction the event reducer should map all states to creation event
        * Reducer can use the Evolver.reduce() to get to final state in which it would derive a creation event for the
          new compacted topic
* [x] Streaming
    * [x] Stream api for usage in event sourcing
* [ ] Performance and benchmarks project "atom-performance"
* [ ] JVM Profiling
* [ ] More structured releases based on PRs

## Release notes

* `20.0.0`
    * Moved reactive out of `memoria-io/atom` repo
    * Nats pull based reactive approach instead of server push
    * Introducing etcd adapter as a KV repo
    * New mechanism for getting latest committed eventId
    * Test coverage increase (especially for the banking Use case)
    * Removal of CommandId, StateId, EventId value objects
        * Despite the benefits of value objects they were hard to maintain and had to copy almost all logic in Id
        * Having to implement Jackson SimpleModule for them meant I needed to create a new project module which inherits
          from EventSourcing and text which was too much of a hassle for couple of value objects

## Contribution

You can just do pull requests, and I will check them asap.

## Related Articles

* [Error handling using Reactor and VAVR](https://marmoush.com/2019/11/12/Error-Handling.html)
* [Why I stopped using getters and setters](https://marmoush.com/2019/12/13/stopped-using-getters-and-setters.html)
