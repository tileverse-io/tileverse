/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

// The environment wins over the profile files, in the SDK and here: these tests need it silent.
@DisabledIfEnvironmentVariable(named = "AWS_USE_FIPS_ENDPOINT", matches = ".+")
@DisabledIfEnvironmentVariable(named = "AWS_USE_DUALSTACK_ENDPOINT", matches = ".+")
@DisabledIfSystemProperty(named = "aws.useFipsEndpoint", matches = ".+")
@DisabledIfSystemProperty(named = "aws.useDualstackEndpoint", matches = ".+")
class ProfileEndpointSettingsTest {

    private static final String STORE_PROFILE = "store";

    private static final AwsCredentialsProvider KEYS =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("access", "secret"));

    // Catches a change of these SDK calls at an upgrade: the presigner must name the host reached by the client.
    @ParameterizedTest(name = "{0}")
    @MethodSource("profileFiles")
    void sameHostAsClient(String scenario, String profileFile, ProfileEndpointSettings expected, String expectedHost) {
        Supplier<ProfileFile> profiles = () -> configFile(profileFile);

        ProfileEndpointSettings settings = ProfileEndpointSettings.resolve(profiles, STORE_PROFILE);

        assertThat(settings).as(scenario).isEqualTo(expected);
        assertThat(presignedHost(settings)).isEqualTo(expectedHost).isEqualTo(clientHost(profiles));
    }

    static Stream<Arguments> profileFiles() {
        return Stream.of(
                Arguments.of("no setting", """
                        [profile store]
                        region = us-east-1
                        """, new ProfileEndpointSettings(false, false), "bucket.s3.amazonaws.com"),
                Arguments.of("FIPS", """
                        [profile store]
                        use_fips_endpoint = true
                        """, new ProfileEndpointSettings(true, false), "bucket.s3-fips.us-east-1.amazonaws.com"),
                Arguments.of("dualstack", """
                        [profile store]
                        use_dualstack_endpoint = true
                        """, new ProfileEndpointSettings(false, true), "bucket.s3.dualstack.us-east-1.amazonaws.com"),
                Arguments.of("FIPS and dualstack", """
                        [profile store]
                        use_fips_endpoint = true
                        use_dualstack_endpoint = true
                        """, new ProfileEndpointSettings(true, true), "bucket.s3-fips.dualstack.us-east-1.amazonaws.com"),
                Arguments.of("settings of the default profile only", """
                        [default]
                        use_fips_endpoint = true
                        use_dualstack_endpoint = true
                        [profile store]
                        region = us-east-1
                        """, new ProfileEndpointSettings(false, false), "bucket.s3.amazonaws.com"));
    }

    private static ProfileFile configFile(String content) {
        return ProfileFile.builder()
                .content(content)
                .type(ProfileFile.Type.CONFIGURATION)
                .build();
    }

    private static String presignedHost(ProfileEndpointSettings settings) {
        S3Presigner.Builder builder = S3Presigner.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(KEYS)
                .fipsEnabled(settings.fips())
                .dualstackEnabled(settings.dualstack());
        try (S3Presigner presigner = builder.build()) {
            return presigner
                    .presignGetObject(request -> request.signatureDuration(Duration.ofMinutes(1))
                            .getObjectRequest(get -> get.bucket("bucket").key("key")))
                    .url()
                    .getHost();
        }
    }

    /** The host of a request sent by the client. {@code S3Utilities} would do a lookup of its own instead. */
    private static String clientHost(Supplier<ProfileFile> profiles) {
        HostRecordingHttpClient http = new HostRecordingHttpClient();
        S3AsyncClient client = S3AsyncClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(KEYS)
                .httpClient(http)
                .overrideConfiguration(
                        override -> override.defaultProfileFileSupplier(profiles).defaultProfileName(STORE_PROFILE))
                .build();
        try (client) {
            CompletableFuture<?> head = client.headObject(request -> request.bucket("bucket")
                    .key("key"));
            return head.orTimeout(10, TimeUnit.SECONDS)
                    .handle((response, failure) -> http.host)
                    .join();
        }
    }

    /** Records the host of a request and fails it. */
    private static final class HostRecordingHttpClient implements SdkAsyncHttpClient {

        private volatile @Nullable String host;

        @Override
        public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
            host = request.request().host();
            UnsupportedOperationException failure = new UnsupportedOperationException("no response");
            request.responseHandler().onError(failure);
            return CompletableFuture.failedFuture(failure);
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
