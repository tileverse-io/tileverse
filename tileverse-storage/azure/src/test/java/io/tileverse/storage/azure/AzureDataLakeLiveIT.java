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
package io.tileverse.storage.azure;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.tck.StorageTCK;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;

/**
 * StorageTCK against a live Azure Data Lake Storage Gen2 account.
 *
 * <p>Azurite does not implement the DFS endpoint, so DataLake-specific guarantees (atomic rename, real directories, HNS
 * path semantics) cannot be exercised in the in-Docker test suite. This IT fills that gap when run against a real
 * account.
 *
 * <p>To enable: set {@code TILEVERSE_AZURE_DFS_URL} to a writable container or sub-prefix, e.g.
 * {@code https://account.dfs.core.windows.net/tileverse-it}. Authentication uses
 * {@link com.azure.identity.DefaultAzureCredential} (env vars, managed identity, Azure CLI). When the env var is unset
 * (the default in CI), all tests are assumption-skipped.
 *
 * <p>Each test runs against a unique random prefix under the configured base URI, so the IT is safe to run concurrently
 * against the same account.
 */
class AzureDataLakeLiveIT extends StorageTCK {

    private static String dfsUrl;
    private static AzureClientCache cache;
    private static URI baseUri;

    @BeforeAll
    static void setUpClass() {
        dfsUrl = System.getenv("TILEVERSE_AZURE_DFS_URL");
        assumeTrue(dfsUrl != null && !dfsUrl.isBlank(), "TILEVERSE_AZURE_DFS_URL not set; skipping live DataLake IT");
        cache = new AzureClientCache();
        baseUri = URI.create(dfsUrl.endsWith("/") ? dfsUrl : dfsUrl + "/");
    }

    private static AzureClientCache.Key keyFor(AzureBlobLocation location) {
        AzureRetryConfig retryConfig =
                new AzureRetryConfig(3, Duration.ofSeconds(4), Duration.ofMinutes(2), Duration.ofSeconds(60));
        return AzureClientCache.key(location.endpoint(), location.accountName(), null, null, null, false, retryConfig);
    }

    @Override
    protected Storage openStorage() throws IOException {
        String testPrefix = "tck-" + UUID.randomUUID().toString().substring(0, 12) + "/";
        URI testUri = baseUri.resolve(testPrefix);
        AzureBlobLocation location = AzureBlobLocation.parse(testUri);
        AzureClientCache.Lease lease = cache.acquire(keyFor(location));
        return new AzureDataLakeStorage(testUri, location, lease);
    }

    @Override
    protected void cleanUp(Storage s) throws IOException {
        try (Stream<StorageEntry> stream = s.list("**")) {
            List<String> all = stream.filter(e -> e instanceof StorageEntry.File)
                    .map(StorageEntry::key)
                    .toList();
            if (!all.isEmpty()) {
                s.deleteAll(all);
            }
        } catch (Exception ignored) {
            // best-effort
        }
    }
}
