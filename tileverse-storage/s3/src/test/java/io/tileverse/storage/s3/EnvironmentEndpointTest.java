/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.http.async.AsyncExecuteRequest;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

// The variables and system properties of the process come before the profile files, and AWS_PROFILE picks the profile.
@DisabledIfEnvironmentVariable(named = "AWS_ENDPOINT_URL", matches = ".+")
@DisabledIfEnvironmentVariable(named = "AWS_ENDPOINT_URL_S3", matches = ".+")
@DisabledIfEnvironmentVariable(named = "AWS_PROFILE", matches = ".+")
@DisabledIfSystemProperty(named = "aws.endpointUrl", matches = ".+")
@DisabledIfSystemProperty(named = "aws.endpointUrlS3", matches = ".+")
class EnvironmentEndpointTest {

    /** Building a client sends no request: the builder needs an HTTP client, never used here. */
    private static final SdkAsyncHttpClient NO_HTTP = new SdkAsyncHttpClient() {
        @Override
        public CompletableFuture<Void> execute(AsyncExecuteRequest request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("no request expected"));
        }

        @Override
        public void close() {
            // nothing to release
        }
    };

    @ParameterizedTest(name = "{0}")
    @MethodSource("profileFiles")
    void sameEndpointAsClientBuilder(String scenario, String profileFile, Optional<URI> expected) {
        Supplier<ProfileFile> profiles = () -> configFile(profileFile);

        Optional<URI> found = EnvironmentEndpoint.resolve(profiles);
        Optional<URI> foundByBuilder = clientBuilderEndpoint(profiles);

        assertThat(found).as(scenario).isEqualTo(expected).isEqualTo(foundByBuilder);
    }

    static Stream<Arguments> profileFiles() {
        Optional<URI> minio = Optional.of(URI.create("http://minio.lan:9000"));
        return Stream.of(
                Arguments.of("no endpoint", """
                        [default]
                        region = eu-west-1
                        """, Optional.empty()),
                Arguments.of("endpoint of the profile", """
                        [default]
                        endpoint_url = http://minio.lan:9000
                        """, minio),
                Arguments.of("S3 endpoint of the profile", """
                        [default]
                        s3 =
                          endpoint_url = http://minio.lan:9000
                        """, minio),
                Arguments.of("services section", """
                        [default]
                        services = lab
                        [services lab]
                        s3 =
                          endpoint_url = http://minio.lan:9000
                        """, minio),
                Arguments.of("endpoint of another service", """
                        [default]
                        services = lab
                        [services lab]
                        sqs =
                          endpoint_url = http://minio.lan:9000
                        """, Optional.empty()),
                Arguments.of("endpoints ignored", """
                        [default]
                        ignore_configured_endpoint_urls = true
                        endpoint_url = http://minio.lan:9000
                        """, Optional.empty()));
    }

    private static ProfileFile configFile(String content) {
        return ProfileFile.builder()
                .content(content)
                .type(ProfileFile.Type.CONFIGURATION)
                .build();
    }

    private static Optional<URI> clientBuilderEndpoint(Supplier<ProfileFile> profiles) {
        S3AsyncClient client = S3AsyncClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .httpClient(NO_HTTP)
                .overrideConfiguration(override -> override.defaultProfileFileSupplier(profiles))
                .build();
        try (client) {
            return client.serviceClientConfiguration().endpointOverride();
        }
    }
}
