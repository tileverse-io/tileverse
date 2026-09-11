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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import java.io.IOException;
import java.net.URI;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The source identifier of an S3 reader partitions the shared range cache, hence it has to differ whenever the bytes
 * could differ: the same bucket and key on two endpoints are two different objects. Opening a reader issues no request,
 * which lets these tests build real SDK clients against endpoints that do not exist.
 */
class S3StorageSourceIdentifierTest {

    private static final String KEY = "tiles/file.pmtiles";

    private final S3StorageProvider provider = new S3StorageProvider();

    @Test
    void defaultEndpointRendersTheCanonicalS3Uri() throws IOException {
        try (Storage storage = open("s3://bucket/prefix/", null)) {
            assertThat(identifierOf(storage)).isEqualTo("s3://bucket/prefix/" + KEY);
        }
    }

    @Test
    void endpointParameterIsPartOfTheIdentifier() throws IOException {
        try (Storage storage = open("s3://bucket/prefix/", "http://localhost:9000")) {
            assertThat(identifierOf(storage)).isEqualTo("http://localhost:9000/bucket/prefix/" + KEY);
        }
    }

    @Test
    void endpointFromThePathStyleBaseUriIsPartOfTheIdentifier() throws IOException {
        try (Storage storage = open("http://localhost:9000/bucket/prefix/", null)) {
            assertThat(identifierOf(storage)).isEqualTo("http://localhost:9000/bucket/prefix/" + KEY);
        }
    }

    @Test
    void sameKeyOnDifferentEndpointsGetsDifferentIdentifiers() throws IOException {
        try (Storage minio = open("s3://bucket/prefix/", "http://minio:9000");
                Storage ceph = open("s3://bucket/prefix/", "http://ceph:7480")) {
            assertThat(identifierOf(minio)).isNotEqualTo(identifierOf(ceph));
        }
    }

    @Test
    void sameKeyOnTheSameEndpointGetsTheSameIdentifier() throws IOException {
        try (Storage first = open("s3://bucket/prefix/", "http://minio:9000");
                Storage second = open("s3://bucket/prefix/", "http://minio:9000")) {
            assertThat(identifierOf(first)).isEqualTo(identifierOf(second));
        }
    }

    @Test
    void trailingSlashOnTheEndpointDoesNotSplitTheIdentifier() throws IOException {
        try (Storage bare = open("s3://bucket/prefix/", "http://minio:9000");
                Storage slashed = open("s3://bucket/prefix/", "http://minio:9000/")) {
            assertThat(identifierOf(slashed)).isEqualTo(identifierOf(bare));
        }
    }

    @Test
    void borrowedClientEndpointIsPartOfTheIdentifier() throws IOException {
        try (S3Client client = S3Client.builder()
                        .region(Region.US_EAST_1)
                        .endpointOverride(URI.create("http://minio:9000"))
                        .credentialsProvider(AnonymousCredentialsProvider.create())
                        .forcePathStyle(true)
                        .build();
                Storage storage = S3StorageProvider.open(URI.create("s3://bucket/prefix/"), client)) {
            assertThat(identifierOf(storage)).isEqualTo("http://minio:9000/bucket/prefix/" + KEY);
        }
    }

    @Test
    void borrowedClientThatHidesItsConfigurationFallsBackToTheCanonicalS3Uri() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.serviceClientConfiguration()).thenThrow(new UnsupportedOperationException());
        try (Storage storage = S3StorageProvider.open(URI.create("s3://bucket/prefix/"), client)) {
            assertThat(identifierOf(storage)).isEqualTo("s3://bucket/prefix/" + KEY);
        }
    }

    private Storage open(String baseUri, String endpoint) {
        StorageConfig config = new StorageConfig(baseUri).setParameter(S3StorageProvider.S3_ANONYMOUS, true);
        if (endpoint != null) {
            config.setParameter(S3StorageProvider.S3_ENDPOINT, URI.create(endpoint));
        }
        return provider.createStorage(config);
    }

    private static String identifierOf(Storage storage) throws IOException {
        try (RangeReader reader = storage.openRangeReader(KEY)) {
            return reader.getSourceIdentifier();
        }
    }
}
