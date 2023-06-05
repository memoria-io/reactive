package io.memoria.reactive.core.file;

import io.vavr.collection.List;
import org.junit.jupiter.api.*;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class FileOpsTest {
  private static final Path TEST_DIR = Path.of("/tmp/rFilesTest");
  private static final Path SOME_FILE_PATH = TEST_DIR.resolve("file.txt");

  @BeforeEach
  void beforeEach() {
    FileOps.deleteDir(TEST_DIR).subscribe();
  }

  @Test
  @DisplayName("Should create parent dirs")
  void createParents() throws IOException {
    // Given
    var filePath = TEST_DIR.resolve("childDir").resolve("grandChildDir").resolve("file.txt");
    // When
    StepVerifier.create(FileOps.write(filePath, "hello world")).expectNext(filePath).verifyComplete();
    // Then
    var str = new String(Files.readAllBytes(filePath));
    Assertions.assertEquals("hello world", str);
  }

  @Test
  void deleteManyFiles() {
    // Given
    var nDirs = 3;
    var nFiles = 5;
    var totalDeleted = (nDirs * nFiles) + nDirs + 1;
    // Create files
    Flux.range(0, nDirs).map(i -> TEST_DIR.resolve(i + "")).concatMap(dir -> createSomeFiles(dir, nFiles)).subscribe();
    // When
    var deleteFiles = FileOps.deleteDir(TEST_DIR);
    // Then
    StepVerifier.create(deleteFiles).expectNextCount(totalDeleted).verifyComplete();
  }

  @Test
  void deleteOneFile() throws IOException {
    // Given
    FileOps.createDir(TEST_DIR).subscribe();
    Files.createFile(SOME_FILE_PATH);
    // When
    var deleteFile = FileOps.deleteFile(SOME_FILE_PATH);
    // Then
    StepVerifier.create(deleteFile).expectNext(SOME_FILE_PATH).verifyComplete();
  }

  @Test
  void lastFile() {
    // Given
    var files = createSomeFiles(TEST_DIR, 10).collectList().block();
    // When
    var lastFile = FileOps.lastModified(TEST_DIR);
    // Then
    assert files != null;
    StepVerifier.create(lastFile).expectNext(List.ofAll(files).last()).verifyComplete();
  }

  @Test
  void list() {
    // Given
    FileOps.createDir(TEST_DIR).subscribe();
    var listFlux = FileOps.list(TEST_DIR);
    // Then
    StepVerifier.create(listFlux).expectNext().verifyComplete();
    StepVerifier.create(listFlux.count()).expectNext(0L).verifyComplete();
  }

  @Test
  void read() throws IOException {
    // Given
    FileOps.createDir(TEST_DIR).subscribe();
    Files.writeString(SOME_FILE_PATH, "welcome");
    // When
    var read = FileOps.read(SOME_FILE_PATH);
    // Then
    StepVerifier.create(read).expectNext("welcome").verifyComplete();
  }

  @Test
  void readLines() throws IOException {
    // Given
    FileOps.createDir(TEST_DIR).subscribe();
    Files.writeString(SOME_FILE_PATH, "welcome\nhello");
    // When
    var read = FileOps.readLines(SOME_FILE_PATH);
    // Then
    StepVerifier.create(read).expectNext("welcome", "hello").verifyComplete();
  }

  @Test
  @DisplayName("Should create a new file")
  void rewrite() throws IOException {
    // When
    StepVerifier.create(FileOps.rewrite(SOME_FILE_PATH, "hello world")).expectNext(SOME_FILE_PATH).verifyComplete();
    StepVerifier.create(FileOps.rewrite(SOME_FILE_PATH, "hi world")).expectNext(SOME_FILE_PATH).verifyComplete();
    // Then
    var str = new String(Files.readAllBytes(SOME_FILE_PATH));
    Assertions.assertEquals("hi world", str);
  }

  @Test
  @DisplayName("Should create a new file")
  void write() throws IOException {
    // When
    StepVerifier.create(FileOps.write(SOME_FILE_PATH, "hello world")).expectNext(SOME_FILE_PATH).verifyComplete();
    // Then
    var str = new String(Files.readAllBytes(SOME_FILE_PATH));
    Assertions.assertEquals("hello world", str);
  }

  private Flux<Path> createSomeFiles(Path path, int count) {
    return Flux.range(0, count).concatMap(i -> FileOps.write(path.resolve(i + ".json"), "hi" + i));

  }
}
