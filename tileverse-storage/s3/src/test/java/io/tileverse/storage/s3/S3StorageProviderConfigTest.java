/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageFactory;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.util.ReadsSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.regions.Region;

/**
 * Verifies {@link S3StorageProvider#keyFor(StorageConfig, EnvironmentEndpoint, EnvironmentRegion)} resolution
 * precedence: region, endpoint override, and credential parameters. Replaces the reflection-based
 * {@code testResolveCredentialsProviderPrecedence} / {@code testResolveRegionUsesExplicitAndParsedValues} cases that
 * lived in the now-removed {@code S3RangeReader.Builder}.
 */
// Opening a store reads the system property that disables the S3 provider, set by S3StorageProviderTest
@ReadsSystemProperty
class S3StorageProviderConfigTest {

    /** The path of a path-style URL of {@code data/file.txt} in {@code my-bucket}: the bucket comes first. */
    private static final String PATH_STYLE = "/my-bucket/data/file.txt";

    /** The path of its virtual-hosted-style URL: the bucket is in the host name. */
    private static final String VIRTUAL_HOSTED_STYLE = "/data/file.txt";

    private static final URI MINIO = URI.create("http://minio.lan:9000");

    private static final EnvironmentEndpoint NO_ENVIRONMENT_ENDPOINT = new EnvironmentEndpoint(Optional::empty);

    private static final EnvironmentEndpoint MINIO_IN_ENVIRONMENT = new EnvironmentEndpoint(() -> Optional.of(MINIO));

    private static final Map<String, String> NO_PROFILE_REGIONS = Map.of();

    private static final EnvironmentRegion EMPTY_ENVIRONMENT =
            EnvironmentRegionTest.environment(null, NO_PROFILE_REGIONS, null);

    private static final EnvironmentRegion FAILING_WHEN_ASKED = new EnvironmentRegion(
            S3StorageProviderConfigTest::failWhenAsked,
            profile -> S3StorageProviderConfigTest::failWhenAsked,
            S3StorageProviderConfigTest::failWhenAsked);

    private static final EnvironmentRegion IN_EU_WEST_1 =
            EnvironmentRegionTest.environment("eu-west-1", NO_PROFILE_REGIONS, null);

    private static final EnvironmentRegion ON_EC2_IN_EU_WEST_3 =
            EnvironmentRegionTest.environment(null, NO_PROFILE_REGIONS, "eu-west-3");

    @Test
    void regionResolution_explicitParameterWins() {
        StorageConfig config = new StorageConfig("https://my-bucket.s3.us-west-2.amazonaws.com/file.txt")
                .setParameter(S3StorageProvider.S3_REGION, "us-east-1");

        assertThat(keyFor(config).region()).isEqualTo("us-east-1");
    }

    @Test
    void regionResolution_fallsBackToUriParsedRegion() {
        StorageConfig config = new StorageConfig("https://my-bucket.s3.us-west-2.amazonaws.com/file.txt");

        assertThat(keyFor(config).region()).isEqualTo("us-west-2");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("regions")
    void regionOrder(String scenario, StorageConfig config, EnvironmentRegion environment, String expected) {
        S3ClientCache.Key key = S3StorageProvider.keyFor(config, NO_ENVIRONMENT_ENDPOINT, environment);

        assertThat(key.region()).as(scenario).isEqualTo(expected);
    }

    static Stream<Arguments> regions() {
        String bucket = "s3://my-bucket/file.txt";
        String bucketInUsWest2 = "https://my-bucket.s3.us-west-2.amazonaws.com/file.txt";
        EnvironmentRegion analyticsInApSouth1 = EnvironmentRegionTest.environment(
                null, Map.of("default", "eu-west-2", "analytics", "ap-south-1"), null);
        return Stream.of(
                Arguments.of(
                        "region parameter, environment",
                        new StorageConfig(bucket).setParameter(S3StorageProvider.S3_REGION, "us-west-2"),
                        IN_EU_WEST_1,
                        "us-west-2"),
                Arguments.of(
                        "region in the URL, environment",
                        new StorageConfig(bucketInUsWest2),
                        IN_EU_WEST_1,
                        "us-west-2"),
                Arguments.of("environment", new StorageConfig(bucket), IN_EU_WEST_1, "eu-west-1"),
                Arguments.of("nothing names a region", new StorageConfig(bucket), EMPTY_ENVIRONMENT, "us-east-1"),
                Arguments.of(
                        "profile of the store",
                        new StorageConfig(bucket)
                                .setParameter(S3StorageProvider.S3_DEFAULT_CREDENTIALS_PROFILE, "analytics"),
                        analyticsInApSouth1,
                        "ap-south-1"),
                Arguments.of(
                        "default credentials chain on EC2",
                        new StorageConfig(bucket),
                        ON_EC2_IN_EU_WEST_3,
                        "eu-west-3"),
                Arguments.of(
                        "anonymous store on EC2",
                        new StorageConfig(bucket).setParameter(S3StorageProvider.S3_ANONYMOUS, true),
                        ON_EC2_IN_EU_WEST_3,
                        "us-east-1"),
                Arguments.of(
                        "store with access keys on EC2",
                        withStaticCredentials(new StorageConfig(bucket)),
                        ON_EC2_IN_EU_WEST_3,
                        "us-east-1"));
    }

    // EC2 metadata takes seconds to give up outside EC2: a store on MinIO, R2 or another service must never ask.
    @ParameterizedTest(name = "{0}")
    @MethodSource("endpointOverrides")
    void endpointOverrideSkipsEnvironment(String scenario, StorageConfig config, EnvironmentEndpoint endpoint) {
        S3ClientCache.Key key = S3StorageProvider.keyFor(config, endpoint, FAILING_WHEN_ASKED);

        assertThat(key.region()).as(scenario).isEqualTo("us-east-1");
    }

    static Stream<Arguments> endpointOverrides() {
        URI minio = URI.create("http://localhost:9000");
        StorageConfig endpointParameter =
                new StorageConfig("s3://my-bucket/file.txt").setParameter(S3StorageProvider.S3_ENDPOINT, minio);
        StorageConfig endpointUrl = new StorageConfig("http://localhost:9000/my-bucket/file.txt");
        StorageConfig bucket = new StorageConfig("s3://my-bucket/file.txt");
        return Stream.of(
                Arguments.of("endpoint parameter", endpointParameter, NO_ENVIRONMENT_ENDPOINT),
                Arguments.of("endpoint URL", endpointUrl, NO_ENVIRONMENT_ENDPOINT),
                Arguments.of("endpoint of the environment", bucket, MINIO_IN_ENVIRONMENT));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("endpoints")
    void endpointOrder(String scenario, StorageConfig config, EnvironmentEndpoint endpoint, Optional<URI> expected) {
        S3ClientCache.Key key = S3StorageProvider.keyFor(config, endpoint, EMPTY_ENVIRONMENT);

        assertThat(key.endpointOverride()).as(scenario).isEqualTo(expected);
    }

    static Stream<Arguments> endpoints() {
        URI garage = URI.create("http://garage.lan:3900");
        StorageConfig endpointParameter =
                new StorageConfig("s3://my-bucket/file.txt").setParameter(S3StorageProvider.S3_ENDPOINT, garage);
        StorageConfig endpointUrl = new StorageConfig("http://garage.lan:3900/my-bucket/file.txt");
        StorageConfig bucket = new StorageConfig("s3://my-bucket/file.txt");
        return Stream.of(
                Arguments.of(
                        "endpoint parameter, environment",
                        endpointParameter,
                        MINIO_IN_ENVIRONMENT,
                        Optional.of(garage)),
                Arguments.of("endpoint URL, environment", endpointUrl, MINIO_IN_ENVIRONMENT, Optional.of(garage)),
                Arguments.of("environment", bucket, MINIO_IN_ENVIRONMENT, Optional.of(MINIO)),
                Arguments.of("no endpoint", bucket, NO_ENVIRONMENT_ENDPOINT, Optional.empty()));
    }

    // An endpoint set by AWS_ENDPOINT_URL or a profile is a custom endpoint, like storage.s3.endpoint.
    @Test
    void environmentEndpointAsOverride() {
        S3ClientCache.Key key = S3StorageProvider.keyFor(awsBucket(), MINIO_IN_ENVIRONMENT, FAILING_WHEN_ASKED);

        assertThat(key.forcePathStyle()).isTrue();
        assertThat(key.crossRegionAccess()).isFalse();
    }

    // The host of a presigned URL names the region of the client.
    @Test
    void storageOpensInEnvironmentRegion() throws IOException {
        S3StorageProvider provider = new S3StorageProvider(new S3ClientCache(), NO_ENVIRONMENT_ENDPOINT, IN_EU_WEST_1);

        try (Storage storage = provider.createStorage(awsBucket())) {
            URI objectUrl = storage.presignGet("file.txt", Duration.ofMinutes(1));

            assertThat(objectUrl.getHost()).isEqualTo("my-bucket.s3.eu-west-1.amazonaws.com");
        }
    }

    @Test
    void regionResolution_blankExplicitRegionFallsBack() {
        StorageConfig config = new StorageConfig("https://my-bucket.s3.us-west-2.amazonaws.com/file.txt")
                .setParameter(S3StorageProvider.S3_REGION, "   ");

        assertThat(keyFor(config).region()).isEqualTo("us-west-2");
    }

    @Test
    void endpointOverride_extractedFromCustomHost() {
        StorageConfig config = new StorageConfig("http://localhost:9000/my-bucket/file.txt");

        assertThat(keyFor(config).endpointOverride()).hasValue(URI.create("http://localhost:9000"));
    }

    @Test
    void endpointOverride_emptyForCanonicalAwsUri() {
        StorageConfig config = new StorageConfig("s3://my-bucket/file.txt");

        assertThat(keyFor(config).endpointOverride()).isEmpty();
    }

    @Test
    void endpointOverride_explicitParameterUsedForCanonicalS3Uri() {
        StorageConfig config = new StorageConfig("s3://my-bucket/file.txt")
                .setParameter(S3StorageProvider.S3_ENDPOINT, URI.create("http://localhost:9000"));

        assertThat(keyFor(config).endpointOverride()).hasValue(URI.create("http://localhost:9000"));
    }

    @Test
    void endpointOverride_explicitParameterWinsOverUriDerivedEndpoint() {
        StorageConfig config = new StorageConfig("http://localhost:9000/my-bucket/file.txt")
                .setParameter(S3StorageProvider.S3_ENDPOINT, URI.create("https://minio.example.com"));

        assertThat(keyFor(config).endpointOverride()).hasValue(URI.create("https://minio.example.com"));
    }

    @Test
    void differentEndpointsProduceDifferentCacheKeys() {
        StorageConfig a = new StorageConfig("s3://my-bucket/file.txt")
                .setParameter(S3StorageProvider.S3_ENDPOINT, URI.create("http://localhost:9000"));
        StorageConfig b = new StorageConfig("s3://my-bucket/file.txt")
                .setParameter(S3StorageProvider.S3_ENDPOINT, URI.create("http://localhost:9001"));

        assertThat(keyFor(a)).isNotEqualTo(keyFor(b));
    }

    @Test
    void credentials_staticAccessAndSecretKeyAreCarriedOnTheKey() {
        StorageConfig config = new StorageConfig("s3://my-bucket/file.txt")
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-static")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "secret");

        S3ClientCache.Key key = keyFor(config);
        assertThat(key.accessKeyId()).hasValue("AKIA-static");
        assertThat(key.secretAccessKey()).hasValue("secret");
    }

    @Test
    void credentials_profileNameCarried() {
        StorageConfig config = new StorageConfig("s3://my-bucket/file.txt")
                .setParameter(S3StorageProvider.S3_DEFAULT_CREDENTIALS_PROFILE, "production");

        assertThat(keyFor(config).profile()).hasValue("production");
    }

    @Test
    void credentials_emptyWhenNoCredentialsConfigured() {
        StorageConfig config = new StorageConfig("s3://my-bucket/file.txt");

        S3ClientCache.Key key = keyFor(config);
        assertThat(key.accessKeyId()).isEmpty();
        assertThat(key.secretAccessKey()).isEmpty();
        assertThat(key.profile()).isEmpty();
    }

    // Opened like a caller does: StorageFactory merges the declared defaults of the provider into the config first.
    @ParameterizedTest(name = "{0}")
    @MethodSource("addressingStyles")
    void addressingStyle(String scenario, StorageConfig config, String objectUrlPath) throws IOException {
        try (Storage storage = StorageFactory.open(config)) {
            URI objectUrl = storage.presignGet("file.txt", Duration.ofMinutes(1));

            assertThat(objectUrl.getPath()).as(scenario).isEqualTo(objectUrlPath);
        }
    }

    static Stream<Arguments> addressingStyles() {
        return Stream.of(
                Arguments.of("AWS bucket", awsBucket(), VIRTUAL_HOSTED_STYLE),
                Arguments.of("AWS bucket, path style on", pathStyle(awsBucket(), true), PATH_STYLE),
                Arguments.of("AWS bucket, path style off", pathStyle(awsBucket(), false), VIRTUAL_HOSTED_STYLE),
                Arguments.of("endpoint parameter", endpointParameter(), PATH_STYLE),
                Arguments.of("endpoint parameter, path style on", pathStyle(endpointParameter(), true), PATH_STYLE),
                Arguments.of(
                        "endpoint parameter, path style off",
                        pathStyle(endpointParameter(), false),
                        VIRTUAL_HOSTED_STYLE),
                Arguments.of("endpoint URL", endpointUrl(), PATH_STYLE),
                Arguments.of("endpoint URL, path style on", pathStyle(endpointUrl(), true), PATH_STYLE),
                Arguments.of("endpoint URL, path style off", pathStyle(endpointUrl(), false), VIRTUAL_HOSTED_STYLE));
    }

    private static StorageConfig awsBucket() {
        StorageConfig config = new StorageConfig("s3://my-bucket/data/");
        return withStaticCredentials(config);
    }

    private static StorageConfig endpointParameter() {
        return awsBucket().setParameter(S3StorageProvider.S3_ENDPOINT, URI.create("http://localhost:9000"));
    }

    private static StorageConfig endpointUrl() {
        StorageConfig config = new StorageConfig("http://localhost:9000/my-bucket/data/");
        config.providerId(S3StorageProvider.ID);
        return withStaticCredentials(config);
    }

    private static StorageConfig pathStyle(StorageConfig config, boolean forced) {
        return config.setParameter(S3StorageProvider.S3_FORCE_PATH_STYLE, forced);
    }

    // Presigning needs credentials, and static keys resolve without reading the environment.
    private static StorageConfig withStaticCredentials(StorageConfig config) {
        return config.setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-static")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "secret");
    }

    @Test
    void sameKeyIsReturnedForEquivalentConfigs() {
        StorageConfig a = new StorageConfig("s3://my-bucket/file.txt")
                .setParameter(S3StorageProvider.S3_REGION, "us-west-2")
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-X")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "secret");
        StorageConfig b = new StorageConfig("s3://my-bucket/different-key.txt")
                .setParameter(S3StorageProvider.S3_REGION, "us-west-2")
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-X")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "secret");

        // Cache discriminator is independent of the per-object key (different files in same bucket
        // share the same SDK client, by design).
        assertThat(keyFor(a)).isEqualTo(keyFor(b));
    }

    @Test
    void differentRegionsProduceDifferentCacheKeys() {
        StorageConfig a =
                new StorageConfig("s3://my-bucket/file.txt").setParameter(S3StorageProvider.S3_REGION, "us-west-2");
        StorageConfig b =
                new StorageConfig("s3://my-bucket/file.txt").setParameter(S3StorageProvider.S3_REGION, "eu-central-1");

        assertThat(keyFor(a)).isNotEqualTo(keyFor(b));
    }

    @Test
    void differentAccessKeysProduceDifferentCacheKeys() {
        URI uri = URI.create("s3://my-bucket/file.txt");
        StorageConfig a = new StorageConfig(uri)
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-A")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "s");
        StorageConfig b = new StorageConfig(uri)
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-B")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "s");

        S3ClientCache.Key keyA = keyFor(a);
        S3ClientCache.Key keyB = keyFor(b);
        assertThat(keyA.accessKeyId()).isEqualTo(Optional.of("AKIA-A"));
        assertThat(keyB.accessKeyId()).isEqualTo(Optional.of("AKIA-B"));
        assertThat(keyA).isNotEqualTo(keyB);
    }

    private static Region failWhenAsked() {
        throw new AssertionError("the environment was asked for a region");
    }

    /** The key of a store in an environment naming no endpoint and no region. */
    private static S3ClientCache.Key keyFor(StorageConfig config) {
        return S3StorageProvider.keyFor(config, NO_ENVIRONMENT_ENDPOINT, EMPTY_ENVIRONMENT);
    }
}
