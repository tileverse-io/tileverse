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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.Storage;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Live AWS smoke test for S3 Express (Directory Buckets). Set {@code TILEVERSE_S3_EXPRESS_BUCKET} in the environment to
 * enable. The bucket must follow the Express naming convention {@code <name>--<az>--x-s3} and must be reachable with
 * the ambient AWS credentials.
 */
class S3ExpressLiveIT {

    private static String bucketName;
    private static S3ClientCache cache;
    private static Storage storage;

    @BeforeAll
    static void setUp() {
        bucketName = System.getenv("TILEVERSE_S3_EXPRESS_BUCKET");
        assumeTrue(
                bucketName != null && !bucketName.isBlank(),
                "TILEVERSE_S3_EXPRESS_BUCKET not set; skipping live S3 Express IT");
        cache = new S3ClientCache();
        URI baseUri = URI.create("s3://" + bucketName + "/");
        S3StorageBucketKey ref = S3StorageBucketKey.parse(baseUri);
        String region = Optional.ofNullable(System.getenv("AWS_REGION")).orElse("us-west-2");
        S3ClientCache.Key key = S3ClientCache.key(region, null, false, null, null, null, false);
        storage = new S3Storage(baseUri, ref, cache.acquire(key));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void capabilitiesReflectExpressBucket() {
        assertThat(((S3Storage) storage).isDirectoryBucket()).isTrue();
        assertThat(storage.capabilities().maxPresignTtl()).contains(Duration.ofMinutes(5));
        assertThat(storage.capabilities().versioning()).isFalse();
    }

    @Test
    void putThenStatThenDelete() {
        String key = "tileverse-it/" + System.currentTimeMillis() + ".bin";
        storage.put(key, "hello".getBytes(StandardCharsets.UTF_8));
        try {
            assertThat(storage.exists(key)).isTrue();
            assertThat(storage.stat(key).orElseThrow().size()).isEqualTo(5);
        } finally {
            storage.delete(key);
        }
    }

    @Test
    void presignTtlRejectedAbove5Minutes() {
        Duration tooLong = Duration.ofMinutes(10);
        assertThatThrownBy(() -> storage.presignGet("anything", tooLong)).isInstanceOf(IllegalArgumentException.class);
    }
}
