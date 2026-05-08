/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.storage.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
        // Pre-populate nothing for read-path tests except a single empty file so the
        // server is available; TCK tests we care about for HttpStorage are stat-missing
        // and read-missing, which don't depend on fixtures.
        Files.writeString(docroot.resolve("placeholder.txt"), "ok", StandardCharsets.UTF_8);

        httpd = new GenericContainer<>(DockerImageName.parse("httpd:alpine"))
                .withExposedPorts(80)
                .withCopyToContainer(
                        MountableFile.forHostPath(docroot.resolve("placeholder.txt")),
                        "/usr/local/apache2/htdocs/placeholder.txt")
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
        return new HttpStorage(base, new BorrowedHttpHandle(HttpClient.newHttpClient()), HttpAuthentication.NONE);
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
}
