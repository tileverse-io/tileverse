/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * IT for HttpStorage. The TCK's write-path tests are gated on {@code requireWrites()} and skip automatically because
 * HttpStorage declares {@code writes=false}. The few read-path tests that don't need writes (stat for missing key,
 * exists, read-not-found) run against an httpd container with a small fixture; the rest are skipped.
 */
@Testcontainers(disabledWithoutDocker = true)
class HttpStorageIT extends StorageTCK {

    static GenericContainer<?> httpd;
    static Path docroot;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startHttpd() throws IOException {
        docroot = Files.createTempDirectory("http-storage-it-");
        // The conditional reads of this class read this file; the TCK tests running here need no fixture.
        Files.writeString(docroot.resolve("since.bin"), "0123456789", StandardCharsets.UTF_8);

        httpd = new GenericContainer<>(DockerImageName.parse("httpd:alpine"))
                .withExposedPorts(80)
                .withCopyToContainer(
                        MountableFile.forHostPath(docroot.resolve("since.bin")),
                        "/usr/local/apache2/htdocs/since.bin")
                .waitingFor(Wait.forHttp("/").forPort(80));
        httpd.start();
    }

    @AfterAll
    static void stopHttpd() throws IOException {
        if (httpd != null) {
            httpd.stop();
        }
        if (docroot != null) {
            try (Stream<Path> s = Files.walk(docroot)) {
                s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort
                    }
                });
            }
        }
    }

    @Override
    protected Storage openStorage() throws IOException {
        URI base = URI.create("http://" + httpd.getHost() + ":" + httpd.getFirstMappedPort() + "/");
        return new HttpStorage(
                base,
                new BorrowedHttpHandle(HttpClient.newHttpClient()),
                HttpAuthentication.NONE,
                BatchSettings.httpDefaults());
    }

    @Test
    void writesDeclaredFalse() {
        assertThat(caps.writes()).isFalse();
    }

    @Test
    void putThrowsUnsupported() {
        assertThatThrownBy(() -> storage.put("k", new byte[1])).isInstanceOf(UnsupportedCapabilityException.class);
    }

    @Test
    void listThrowsUnsupported() {
        assertThatThrownBy(() -> {
                    try (Stream<StorageEntry> s = storage.list("")) {
                        // close immediately
                    }
                })
                .isInstanceOf(UnsupportedCapabilityException.class);
    }

    @Test
    void presignThrowsUnsupported() {
        Duration duration = Duration.ofMinutes(1);
        assertThatThrownBy(() -> storage.presignGet("k", duration)).isInstanceOf(UnsupportedCapabilityException.class);
    }

    // TCK tests write the object they read: HttpStorage reads a file served by the container instead.
    @ParameterizedTest
    @MethodSource("conditionalReads")
    void unmodifiedFileReadFails(long offset, OptionalLong length) {
        Instant lastModified = storage.stat("since.bin").orElseThrow().lastModified();
        ReadOptions since = ifModifiedSince(offset, length, lastModified);

        assertThatThrownBy(() -> storage.read("since.bin", since)).isInstanceOf(PreconditionFailedException.class);
    }

    @ParameterizedTest
    @MethodSource("conditionalReads")
    void modifiedFileReadSucceeds(long offset, OptionalLong length, String expected) throws IOException {
        Instant lastModified = storage.stat("since.bin").orElseThrow().lastModified();
        ReadOptions since = ifModifiedSince(offset, length, lastModified.minus(Duration.ofHours(1)));

        try (ReadHandle handle = storage.read("since.bin", since)) {
            byte[] content = handle.content().readAllBytes();
            assertThat(new String(content, StandardCharsets.UTF_8)).isEqualTo(expected);
        }
    }
}
