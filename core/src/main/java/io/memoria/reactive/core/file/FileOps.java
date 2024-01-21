package io.memoria.reactive.core.file;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

public class FileOps {

  private FileOps() {}

  public static Mono<Path> createDir(Path path) {
    return Mono.fromCallable(() -> path.toFile().mkdirs()).thenReturn(path);
  }

  public static Flux<Path> deleteDir(Path path) {
    return deleteDirFiles(path).concatWith(listDir(path).concatMap(FileOps::deleteDir))
                               .concatWith(FileOps.deleteFile(path));
  }

  public static Flux<Path> deleteDirFiles(Path path) {
    return list(path).flatMap(FileOps::deleteFile);
  }

  public static Mono<Path> deleteFile(Path path) {
    return Mono.fromCallable(() -> Files.deleteIfExists(path)).thenReturn(path);
  }

  public static Mono<Path> lastModified(Path path) {
    return list(path).reduce(FileOps::lastModified);
  }

  public static Flux<Path> list(Path path) {
    return listAll(path).filter(f -> !Files.isDirectory(f)).sort();
  }

  public static Flux<Path> listDir(Path path) {
    return listAll(path).filter(Files::isDirectory).sort();
  }

  public static Mono<String> read(Path path) {
    return Mono.fromCallable(() -> Files.readString(path));
  }

  public static Flux<String> readLines(Path path) {
    return Mono.fromCallable(() -> Files.lines(path)).flatMapMany(Flux::fromStream);
  }

  public static Mono<Path> rewrite(Path path, String content) {
    return Mono.fromCallable(() -> {
      Files.createDirectories(path.getParent());
      return Files.writeString(path, content);
    }).thenReturn(path);
  }

  public static Mono<Path> write(Path path, String content) {
    return Mono.fromCallable(() -> {
      Files.createDirectories(path.getParent());
      return Files.writeString(path, content, CREATE_NEW);
    }).thenReturn(path);
  }

  private static Path lastModified(Path p1, Path p2) {
    return (p1.toFile().lastModified() > p2.toFile().lastModified()) ? p1 : p2;
  }

  private static Flux<Path> listAll(Path path) {
    return Mono.fromCallable(() -> Files.list(path))
               .onErrorResume(NoSuchFileException.class, _ -> Mono.just(Stream.empty()))
               .flatMapMany(Flux::fromStream);
  }
}
