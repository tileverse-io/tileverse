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

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import java.net.URI;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3VersioningIT {

    private static final String BUCKET = "versioned-it";

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.7")).withServices(Service.S3);

    private S3Client adminClient;

    @BeforeAll
    void provisionVersionedBucket() {
        adminClient = S3Client.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(Service.S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .region(Region.of(LOCALSTACK.getRegion()))
                .forcePathStyle(true)
                .build();
        adminClient.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        adminClient.putBucketVersioning(PutBucketVersioningRequest.builder()
                .bucket(BUCKET)
                .versioningConfiguration(VersioningConfiguration.builder()
                        .status(BucketVersioningStatus.ENABLED)
                        .build())
                .build());
    }

    @AfterAll
    void closeAdmin() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    @Test
    void writingTwiceProducesDistinctVersionIds() throws Exception {
        URI baseUri = URI.create("s3://" + BUCKET + "/");
        // Pass the pre-configured client directly so the S3 provider picks up the
        // LocalStack endpoint and credentials without going through StorageFactory's
        // URI-scheme dispatch (which routes http://host:port URIs to HTTP storage).
        try (Storage s = S3StorageProvider.open(baseUri, adminClient)) {
            StorageEntry.File first = s.put("k", "v1".getBytes());
            StorageEntry.File second = s.put("k", "v2".getBytes());

            assertThat(first.versionId()).as("first.versionId").isPresent();
            assertThat(second.versionId()).as("second.versionId").isPresent();
            assertThat(first.versionId()).isNotEqualTo(second.versionId());

            assertThat(first.etag()).as("first.etag").isPresent();
            assertThat(second.etag()).as("second.etag").isPresent();
        }
    }
}
