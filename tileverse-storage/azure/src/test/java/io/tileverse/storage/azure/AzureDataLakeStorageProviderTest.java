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

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.spi.StorageConfig;
import java.net.URI;
import org.junit.jupiter.api.Test;

class AzureDataLakeStorageProviderTest {

    @Test
    void canProcessAcceptsAbfsUris() {
        AzureDataLakeStorageProvider p = new AzureDataLakeStorageProvider();
        assertThat(p.canProcess(new StorageConfig().uri(URI.create("abfss://fs@acct.dfs.core.windows.net/path/"))))
                .isTrue();
        assertThat(p.canProcess(new StorageConfig().uri(URI.create("abfs://fs@acct.dfs.core.windows.net/"))))
                .isTrue();
    }

    @Test
    void canProcessAcceptsHttpsToDfsHost() {
        AzureDataLakeStorageProvider p = new AzureDataLakeStorageProvider();
        assertThat(p.canProcess(new StorageConfig().uri(URI.create("https://acct.dfs.core.windows.net/fs/path"))))
                .isTrue();
    }

    @Test
    void canProcessRejectsBlobHost() {
        AzureDataLakeStorageProvider p = new AzureDataLakeStorageProvider();
        assertThat(p.canProcess(new StorageConfig().uri(URI.create("https://acct.blob.core.windows.net/fs/path"))))
                .isFalse();
    }
}
