/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import java.net.URI;
import java.util.Optional;
import java.util.function.Supplier;
import org.jspecify.annotations.NullMarked;
import software.amazon.awssdk.awscore.endpoint.AwsClientEndpointProvider;
import software.amazon.awssdk.profiles.ProfileFile;

/**
 * The S3 endpoint named by the environment of the process, for a store naming none.
 *
 * <p>The S3 client builder finds this endpoint by itself, in the {@code aws.endpointUrlS3} and {@code aws.endpointUrl}
 * system properties, the {@code AWS_ENDPOINT_URL_S3} and {@code AWS_ENDPOINT_URL} environment variables, then the
 * profile files. It does that while building the client, and the store needs the answer earlier. A store on a custom
 * endpoint uses path style, gets no cross-region access, and keeps the us-east-1 default without asking the environment
 * for a region. These settings are inputs of the builder and parts of the key of the client set. Stores with different
 * settings share client sets by key, and the key of a store is computed before a client exists.
 *
 * <p>This class makes the same SDK call as the builder, to get the same answer without building a client. Like the
 * builder, it reads the profile named by {@code AWS_PROFILE}, or {@code default}, whatever the credentials profile of
 * the store: {@link S3ClientCache} gives the client no profile name. The SDK marks the call as protected API, meant for
 * its generated clients, and a minor release may change it. {@code EnvironmentEndpointTest} compares both answers to
 * catch such a change at an SDK upgrade.
 */
@NullMarked
final class EnvironmentEndpoint {

    /** The environment of the process, with the profile files read by the SDK by default. */
    static final EnvironmentEndpoint INSTANCE =
            new EnvironmentEndpoint(() -> EnvironmentEndpoint.resolve(ProfileFile::defaultProfileFile));

    private final Supplier<Optional<URI>> lookup;

    /** Answers with {@code lookup}; tests pass their own. */
    EnvironmentEndpoint(Supplier<Optional<URI>> lookup) {
        this.lookup = lookup;
    }

    /** The endpoint named by the environment, if any. */
    Optional<URI> find() {
        return lookup.get();
    }

    /** The endpoint found by the S3 client builder in the process and in {@code profileFile}. */
    static Optional<URI> resolve(Supplier<ProfileFile> profileFile) {
        // the settings of S3 passed by the S3 client builder, in DefaultS3BaseClientBuilder
        AwsClientEndpointProvider.Builder s3Endpoint = AwsClientEndpointProvider.builder()
                .serviceEndpointOverrideEnvironmentVariable("AWS_ENDPOINT_URL_S3")
                .serviceEndpointOverrideSystemProperty("aws.endpointUrlS3")
                .serviceProfileProperty("s3")
                .profileFile(profileFile);
        return s3Endpoint.resolveFromOverrides();
    }
}
