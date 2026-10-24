/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import java.util.Objects;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * The AWS default credentials provider chain. When it finds no credentials, its failure names
 * {@link S3StorageProvider#S3_ANONYMOUS}: the chain never falls back to the unsigned requests needed by public buckets.
 */
final class DefaultCredentialsChain implements AwsCredentialsProvider, SdkAutoCloseable {

    private final AwsCredentialsProvider chain;

    DefaultCredentialsChain() {
        this(DefaultCredentialsProvider.builder().build());
    }

    /** Wraps {@code chain}; tests pass a chain failing on demand. */
    DefaultCredentialsChain(AwsCredentialsProvider chain) {
        this.chain = Objects.requireNonNull(chain, "chain");
    }

    @Override
    public AwsCredentials resolveCredentials() {
        try {
            return chain.resolveCredentials();
        } catch (SdkClientException noCredentials) {
            String hint = ". If the bucket is public, set " + S3StorageProvider.S3_ANONYMOUS.key() + "=true";
            throw SdkClientException.builder()
                    .message(noCredentials.getMessage() + hint)
                    .cause(noCredentials)
                    .build();
        }
    }

    @Override
    public void close() {
        if (chain instanceof SdkAutoCloseable closeable) {
            closeable.close();
        }
    }
}
