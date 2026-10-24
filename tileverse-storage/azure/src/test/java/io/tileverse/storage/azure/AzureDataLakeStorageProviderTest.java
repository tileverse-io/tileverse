/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.spi.StorageProvider;
import org.junit.jupiter.api.Test;

class AzureDataLakeStorageProviderTest {

    /** One cache entry holds both clients of an account: the Data Lake and Blob providers share one cache. */
    @Test
    void providersOfTwoLookupsLeaseFromTheCacheOfTheBlobProvider() {
        AzureDataLakeStorageProvider first = lookUpProvider();
        AzureDataLakeStorageProvider second = lookUpProvider();
        AzureBlobStorageProvider blobProvider = new AzureBlobStorageProvider();

        assertThat(second).isNotSameAs(first);
        assertThat(second.clientCache()).isSameAs(first.clientCache());
        assertThat(blobProvider.clientCache()).isSameAs(first.clientCache());
    }

    private static AzureDataLakeStorageProvider lookUpProvider() {
        StorageProvider found =
                StorageProvider.findProvider(AzureDataLakeStorageProvider.ID).orElseThrow();
        return (AzureDataLakeStorageProvider) found;
    }

    @Test
    void canProcessAcceptsAbfsUris() {
        AzureDataLakeStorageProvider p = new AzureDataLakeStorageProvider();
        assertThat(p.canProcess(new StorageConfig("abfss://fs@acct.dfs.core.windows.net/path/")))
                .isTrue();
        assertThat(p.canProcess(new StorageConfig("abfs://fs@acct.dfs.core.windows.net/")))
                .isTrue();
    }

    @Test
    void canProcessAcceptsHttpsToDfsHost() {
        AzureDataLakeStorageProvider p = new AzureDataLakeStorageProvider();
        StorageConfig namingDataLake = new StorageConfig("https://acct.dfs.core.windows.net/fs/path")
                .providerId(AzureDataLakeStorageProvider.ID);
        assertThat(p.canProcess(namingDataLake)).isTrue();
    }

    @Test
    void canProcessRejectsHttpsToDfsHostWithNoProviderNamed() {
        AzureDataLakeStorageProvider p = new AzureDataLakeStorageProvider();
        assertThat(p.canProcess(new StorageConfig("https://acct.dfs.core.windows.net/fs/path")))
                .isFalse();
    }

    @Test
    void canProcessRejectsBlobHost() {
        AzureDataLakeStorageProvider p = new AzureDataLakeStorageProvider();
        StorageConfig namingDataLake = new StorageConfig("https://acct.blob.core.windows.net/fs/path")
                .providerId(AzureDataLakeStorageProvider.ID);
        assertThat(p.canProcess(namingDataLake)).isFalse();
    }

    @Test
    void anAnonymousOpenFailsWithoutKeepingAClient() {
        AzureClientCache clientCache = new AzureClientCache();
        AzureDataLakeStorageProvider provider = new AzureDataLakeStorageProvider(clientCache);
        StorageConfig config = new StorageConfig("https://acct.dfs.core.windows.net/fs/path/")
                .setParameter(AzureBlobStorageProvider.AZURE_ANONYMOUS, true);

        Throwable failure = catchThrowable(() -> provider.createStorage(config));

        assertThat(clientCache.entryCount())
                .as("clients kept by the failed open")
                .isZero();
        assertThat(failure)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(AzureBlobStorageProvider.AZURE_ANONYMOUS.key());
    }
}
