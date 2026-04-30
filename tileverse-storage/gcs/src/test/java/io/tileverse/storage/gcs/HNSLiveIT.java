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
package io.tileverse.storage.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.Storage;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Live GCS smoke test for Hierarchical Namespace buckets. Set {@code TILEVERSE_GCS_HNS_BUCKET} to a real HNS-enabled
 * bucket name to enable. Authentication uses the application default credential chain (env vars, gcloud user creds,
 * GCE/GKE metadata).
 */
class HNSLiveIT {

    private static String bucket;
    private static SdkStorageCache cache;
    private static Storage storage;

    @BeforeAll
    static void setUp() {
        bucket = System.getenv("TILEVERSE_GCS_HNS_BUCKET");
        assumeTrue(bucket != null && !bucket.isBlank(), "TILEVERSE_GCS_HNS_BUCKET not set; skipping live HNS IT");
        cache = new SdkStorageCache();
        URI baseUri = URI.create("gs://" + bucket + "/");
        SdkStorageLocation location = SdkStorageLocation.parse(baseUri);
        SdkStorageCache.Key key = new SdkStorageCache.Key(Optional.empty(), Optional.empty(), Optional.empty(), false);
        storage = new GoogleCloudStorage(baseUri, location, cache.acquire(key));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
    }

    @Test
    void hnsCapabilitiesDetected() {
        assertThat(((GoogleCloudStorage) storage).isHns()).isTrue();
        assertThat(storage.capabilities().realDirectories()).isTrue();
        assertThat(storage.capabilities().atomicMove()).isTrue();
    }

    @Test
    void putThenStatThenDelete() throws Exception {
        String key = "tileverse-it/" + System.currentTimeMillis() + ".bin";
        storage.put(key, "hello".getBytes(StandardCharsets.UTF_8));
        try {
            assertThat(storage.exists(key)).isTrue();
        } finally {
            storage.delete(key);
        }
    }
}
