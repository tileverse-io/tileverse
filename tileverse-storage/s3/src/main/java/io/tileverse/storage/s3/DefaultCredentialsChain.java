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
