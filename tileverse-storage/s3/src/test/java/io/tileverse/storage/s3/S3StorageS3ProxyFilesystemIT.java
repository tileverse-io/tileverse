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
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Reads objects from an s3proxy on a filesystem backend, where a file placed directly in the backend directory is
 * served without an {@code ETag} header. Objects written through the S3 API do have one, which
 * {@link S3StorageS3ProxyIT} covers on the transient backend.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3StorageS3ProxyFilesystemIT {

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("andrewgaul/s3proxy:latest");
    private static final String IDENTITY = "s3proxy-access-key";
    private static final String CREDENTIAL = "s3proxy-secret-key";
    private static final String BACKEND_DIR = "/data";

    /** Large enough for the read offsets below to plan one fetch each. */
    private static final int OBJECT_SIZE = 2 * 1024 * 1024;

    private static final String DIRECT_KEY = "placed-in-the-backend.bin";
    private static final String API_KEY = "written-through-the-api.bin";

    @SuppressWarnings("resource")
    private static GenericContainer<?> s3proxy;

    private static byte[] objectBytes;

    private S3ClientCache cache;
    private S3ClientCache.Lease lease;
    private String bucket;
    private S3Storage storage;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startContainer() {
        s3proxy = new GenericContainer<>(DOCKER_IMAGE_NAME)
                .withExposedPorts(80)
                .withEnv("S3PROXY_AUTHORIZATION", "aws-v2-or-v4")
                .withEnv("S3PROXY_IDENTITY", IDENTITY)
                .withEnv("S3PROXY_CREDENTIAL", CREDENTIAL)
                .withEnv("S3PROXY_ENDPOINT", "http://0.0.0.0:80")
                .withEnv("JCLOUDS_PROVIDER", "filesystem")
                .withEnv("JCLOUDS_FILESYSTEM_BASEDIR", BACKEND_DIR);
        s3proxy.start();
        objectBytes = deterministicBytes(OBJECT_SIZE);
    }

    @AfterAll
    static void stopContainer() {
        if (s3proxy != null) {
            s3proxy.stop();
        }
    }

    private static byte[] deterministicBytes(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i * 31);
        }
        return data;
    }

    private static URI endpoint() {
        return URI.create("http://" + s3proxy.getHost() + ":" + s3proxy.getMappedPort(80));
    }

    private S3ClientCache.Key keyFor() {
        return S3ClientCache.key("us-east-1", endpoint(), false, IDENTITY, CREDENTIAL, null, true);
    }

    /**
     * Places one object directly in the backend directory, the case answered without an ETag, and writes another
     * through the S3 API. A fresh client cache per test means a fresh ETag record.
     */
    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        cache = new S3ClientCache();
        bucket = "fs-" + UUID.randomUUID().toString().substring(0, 12);
        s3proxy.execInContainer("mkdir", "-p", BACKEND_DIR + "/" + bucket);
        s3proxy.copyFileToContainer(Transferable.of(objectBytes), BACKEND_DIR + "/" + bucket + "/" + DIRECT_KEY);

        lease = cache.acquire(keyFor());
        URI baseUri = URI.create("s3://" + bucket + "/");
        storage = new S3Storage(baseUri, S3StorageBucketKey.parse(baseUri), lease, false);
        storage.put(API_KEY, objectBytes, WriteOptions.defaults());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (storage != null) {
            storage.close();
        }
    }

    private static List<RangeRequest> batch() {
        return List.of(
                RangeRequest.of(0L, 4096, ByteBuffer.allocate(4096)),
                RangeRequest.of(600_000L, 4096, ByteBuffer.allocate(4096)),
                RangeRequest.of(1_500_000L, 4096, ByteBuffer.allocate(4096)));
    }

    private static void assertBatchMatchesTheObject(List<RangeRequest> requests, int[] counts) {
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest request = requests.get(i);
            int offset = (int) request.range().offset();
            int length = request.range().length();
            assertThat(counts[i]).as("bytes for entry " + i).isEqualTo(length);
            byte[] read = new byte[length];
            request.target().duplicate().flip().get(read);
            byte[] expected = new byte[length];
            System.arraycopy(objectBytes, offset, expected, 0, length);
            assertThat(read).as("entry " + i).containsExactly(expected);
        }
    }

    @Test
    void statReportsNoEtagForAnObjectPlacedInTheBackend() {
        assertThat(storage.stat(DIRECT_KEY).orElseThrow().etag()).isEmpty();
        assertThat(storage.stat(API_KEY).orElseThrow().etag()).isNotEmpty();
    }

    @Test
    void singleRangeReadsWorkWithoutAnEtag() {
        ByteBuffer target = ByteBuffer.allocate(64);

        int read = storage.openRangeReader(DIRECT_KEY).readRange(0L, 64, target);

        assertThat(read).isEqualTo(64);
        byte[] expected = new byte[64];
        System.arraycopy(objectBytes, 0, expected, 0, 64);
        assertThat(target.array()).containsExactly(expected);
    }

    @Test
    void batchedReadsFallBackWhenTheEndpointOmitsTheEtag() {
        List<RangeRequest> requests = batch();

        int[] counts = storage.openRangeReader(DIRECT_KEY).readRanges(requests);

        assertBatchMatchesTheObject(requests, counts);
        assertThat(lease.endpointEtags().omitted()).isTrue();
    }

    @Test
    void batchedReadsKeepWorkingOnceTheEndpointIsKnownToOmitTheEtag() {
        storage.openRangeReader(DIRECT_KEY).readRanges(batch());
        List<RangeRequest> requests = batch();

        int[] counts = storage.openRangeReader(DIRECT_KEY).readRanges(requests);

        assertBatchMatchesTheObject(requests, counts);
    }

    @Test
    void streamingReadsFallBackWhenTheEndpointOmitsTheEtag() throws IOException {
        try (ReadHandle handle = storage.read(DIRECT_KEY)) {
            assertThat(handle.content().readAllBytes()).containsExactly(objectBytes);
        }
    }

    @Test
    void objectsWithAnEtagLeaveTheEndpointUnmarked() throws IOException {
        List<RangeRequest> requests = batch();

        int[] counts = storage.openRangeReader(API_KEY).readRanges(requests);
        try (ReadHandle handle = storage.read(API_KEY)) {
            assertThat(handle.content().readAllBytes()).containsExactly(objectBytes);
        }

        assertBatchMatchesTheObject(requests, counts);
        assertThat(lease.endpointEtags().omitted()).isFalse();
    }
}
