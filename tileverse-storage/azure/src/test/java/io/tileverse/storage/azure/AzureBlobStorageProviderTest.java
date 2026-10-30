/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.tileverse.storage.PresignWriteOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.batch.BatchProviderHelper;
import io.tileverse.storage.spi.StorageProvider;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AzureBlobStorageProviderTest {

    private final AzureBlobStorageProvider provider = new AzureBlobStorageProvider();

    /** Every lookup instantiates the provider again, as StorageFactory does at every open. */
    @Test
    void providersOfTwoLookupsLeaseFromOneCache() {
        AzureBlobStorageProvider first = lookUpProvider();
        AzureBlobStorageProvider second = lookUpProvider();

        assertThat(second).isNotSameAs(first);
        assertThat(second.clientCache()).isSameAs(first.clientCache());
    }

    private static AzureBlobStorageProvider lookUpProvider() {
        StorageProvider found =
                StorageProvider.findProvider(AzureBlobStorageProvider.ID).orElseThrow();
        return (AzureBlobStorageProvider) found;
    }

    @Test
    void canProcessAcceptsContainerOnlyUri() {
        StorageConfig container = configNamingTheProvider("https://acct.blob.core.windows.net/my-container");
        StorageConfig prefix = configNamingTheProvider("https://acct.blob.core.windows.net/my-container/prefix/");
        StorageConfig blob = configNamingTheProvider("https://acct.blob.core.windows.net/my-container/blob.bin");

        assertThat(provider.canProcess(container)).isTrue();
        assertThat(provider.canProcess(prefix)).isTrue();
        assertThat(provider.canProcess(blob)).isTrue();
    }

    @Test
    void canProcessRejectsHostWithoutContainer() {
        StorageConfig accountRoot = configNamingTheProvider("https://acct.blob.core.windows.net/");

        assertThat(provider.canProcess(accountRoot)).isFalse();
    }

    /** The http(s) forms are claimed only for a config naming the provider. */
    private static StorageConfig configNamingTheProvider(String url) {
        return new StorageConfig(url).providerId(AzureBlobStorageProvider.ID);
    }

    private static final String CONTAINER_URL = "https://acct.blob.core.windows.net/container/";

    /** The well-known account key of the Azurite emulator, valid base64 for signing. */
    private static final String ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    // the SDK signs a SAS with an account key only
    @ParameterizedTest
    @MethodSource("credentialsAndPresigning")
    void presignsWithAccountKeyOnly(Map<String, String> credentials, boolean presigns) throws IOException {
        StorageConfig config = configNamingTheProvider(CONTAINER_URL);
        credentials.forEach(config::setParameter);
        try (Storage storage = provider.createStorage(config)) {
            assertThat(storage.capabilities().presignedUrls()).isEqualTo(presigns);
        }
    }

    static Stream<Arguments> credentialsAndPresigning() {
        String keyConnection = "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=" + ACCOUNT_KEY
                + ";EndpointSuffix=core.windows.net";
        String sasConnection =
                "BlobEndpoint=https://acct.blob.core.windows.net;SharedAccessSignature=sv=2020-08-04&sig=abc";
        return Stream.of(
                arguments(Map.of(AzureBlobStorageProvider.AZURE_ACCOUNT_KEY.key(), ACCOUNT_KEY), true),
                arguments(Map.of(AzureBlobStorageProvider.AZURE_CONNECTION_STRING.key(), keyConnection), true),
                arguments(Map.of(AzureBlobStorageProvider.AZURE_CONNECTION_STRING.key(), sasConnection), false),
                arguments(Map.of(AzureBlobStorageProvider.AZURE_SAS_TOKEN.key(), "sv=2020-08-04&sig=abc"), false),
                arguments(Map.of(AzureBlobStorageProvider.AZURE_ANONYMOUS.key(), "true"), false),
                arguments(Map.of(), false));
    }

    @Test
    void sasTokenStoreRefusesPresign() throws IOException {
        StorageConfig config = configNamingTheProvider(CONTAINER_URL)
                .setParameter(AzureBlobStorageProvider.AZURE_SAS_TOKEN, "sv=2020-08-04&sig=abc");
        Duration ttl = Duration.ofMinutes(5);
        PresignWriteOptions writeOptions = PresignWriteOptions.defaults();
        try (Storage storage = provider.createStorage(config)) {
            assertThatThrownBy(() -> storage.presignGet("blob.bin", ttl))
                    .isInstanceOf(UnsupportedCapabilityException.class);
            assertThatThrownBy(() -> storage.presignPut("blob.bin", ttl, writeOptions))
                    .isInstanceOf(UnsupportedCapabilityException.class);
        }
    }

    @Test
    void accountKeyStorePresigns() throws IOException {
        StorageConfig config = configNamingTheProvider(CONTAINER_URL)
                .setParameter(AzureBlobStorageProvider.AZURE_ACCOUNT_KEY, ACCOUNT_KEY);
        try (Storage storage = provider.createStorage(config)) {
            URI presigned = storage.presignGet("blob.bin", Duration.ofMinutes(5));

            assertThat(presigned.getPath()).isEqualTo("/container/blob.bin");
            assertThat(presigned.getRawQuery()).contains("sig=");
        }
    }

    @Test
    void testFactoryLookup() {
        assertThat(StorageProvider.findProvider(AzureBlobStorageProvider.ID)).isPresent();
        assertThat(StorageProvider.getProvider(AzureBlobStorageProvider.ID, true))
                .isNotNull();
    }

    @Test
    void testBuildParameters() {
        List<StorageParameter<?>> parameters = provider.buildParameters();
        assertThat(parameters)
                .containsExactly(
                        AzureBlobStorageProvider.AZURE_ANONYMOUS,
                        AzureBlobStorageProvider.AZURE_ACCOUNT_KEY,
                        AzureBlobStorageProvider.AZURE_SAS_TOKEN,
                        AzureBlobStorageProvider.AZURE_CONNECTION_STRING,
                        AzureBlobStorageProvider.AZURE_ENDPOINT,
                        AzureBlobStorageProvider.AZURE_MAX_RETRIES,
                        AzureBlobStorageProvider.AZURE_RETRY_DELAY,
                        AzureBlobStorageProvider.AZURE_MAX_RETRY_DELAY,
                        AzureBlobStorageProvider.AZURE_TRY_TIMEOUT,
                        BatchProviderHelper.BATCH_MAX_GAP,
                        BatchProviderHelper.BATCH_MAX_FETCH,
                        BatchProviderHelper.BATCH_MAX_IN_FLIGHT_FETCHES);
    }

    @Test
    void testCanProcess() {
        StorageConfig config = provider.getDefaultConfig().providerId(AzureBlobStorageProvider.ID);

        assertThatThrownBy(() -> provider.canProcess(null)).isInstanceOf(NullPointerException.class);

        assertThat(provider.canProcess(config.baseUri("https://account.blob.core.windows.net/container/blob.pmtiles")))
                .isTrue();
        assertThat(provider.canProcess(config.baseUri("http://127.0.0.1:10000/account/container/blob.pmtiles")))
                .isTrue();

        assertThat(provider.canProcess(config.baseUri("https://account.blob.core.windows.net/container")))
                .isTrue();

        assertThat(provider.canProcess(config.baseUri("ftp://account.blob.core.windows.net/container/blob.pmtiles")))
                .isFalse();
        assertThat(provider.canProcess(config.baseUri("https://example.com"))).isFalse();
    }

    @ParameterizedTest
    @MethodSource("httpUrlForms")
    void canProcessAnHttpUrlOnlyForAConfigNamingTheProvider(String url) {
        StorageConfig namingNoProvider = new StorageConfig(url);
        StorageConfig namingAzure = new StorageConfig(url).providerId(AzureBlobStorageProvider.ID);
        StorageConfig namingHttp = new StorageConfig(url).providerId("http");

        assertThat(provider.canProcess(namingNoProvider))
                .as("no provider named")
                .isFalse();
        assertThat(provider.canProcess(namingAzure)).as("azure named").isTrue();
        assertThat(provider.canProcess(namingHttp)).as("http named").isFalse();
    }

    static Stream<String> httpUrlForms() {
        return Stream.of(
                "https://account.blob.core.windows.net/container",
                "https://account.blob.core.windows.net/container/blob.pmtiles",
                "http://127.0.0.1:10000/account/container/blob.pmtiles");
    }

    @Test
    void canProcessAcceptsAzScheme() {
        assertThat(provider.canProcess(new StorageConfig("az://overturemapswestus2/release")))
                .isTrue();
        assertThat(provider.canProcess(new StorageConfig("az://overturemapswestus2/release/2026-03-18.0/")))
                .isTrue();
        // Account without container is rejected
        assertThat(provider.canProcess(new StorageConfig("az://overturemapswestus2")))
                .isFalse();
        assertThat(provider.canProcess(new StorageConfig("az://overturemapswestus2/")))
                .isFalse();
    }

    @Test
    void azSchemeRoundTripsThroughLocationParser() {
        AzureBlobLocation loc = AzureBlobLocation.parse(URI.create("az://overturemapswestus2/release/2026-03-18.0/"));
        assertThat(loc.accountName()).isEqualTo("overturemapswestus2");
        assertThat(loc.container()).isEqualTo("release");
        assertThat(loc.prefix()).isEqualTo("2026-03-18.0/");
        assertThat(loc.endpoint()).isEqualTo("https://overturemapswestus2.blob.core.windows.net");
    }

    @Test
    void rootContainerIsNamedInTheUrl() {
        URI uri = URI.create("https://account.blob.core.windows.net/$root/");
        AzureBlobLocation location = AzureBlobLocation.parse(uri);

        assertThat(location.container()).isEqualTo("$root");
        assertThat(location.prefix()).isEmpty();
        assertThat(location.resolve("file.pmtiles")).isEqualTo("file.pmtiles");
        assertThat(provider.canProcess(configNamingTheProvider(uri.toString()))).isTrue();
        assertThat(AzureBlobLocation.parse(URI.create("az://account/$root/")).container())
                .isEqualTo("$root");
    }

    @Test
    void azSchemeWithoutPrefixYieldsEmptyPrefix() {
        AzureBlobLocation loc = AzureBlobLocation.parse(URI.create("az://acct/container"));
        assertThat(loc.accountName()).isEqualTo("acct");
        assertThat(loc.container()).isEqualTo("container");
        assertThat(loc.prefix()).isEmpty();
    }
}
