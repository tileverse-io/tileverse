/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.exception.SdkClientException;

class DefaultCredentialsChainTest {

    @Test
    void aChainFindingNoCredentialsNamesTheAnonymousParameter() {
        SdkClientException noCredentials =
                SdkClientException.create("Unable to load credentials from any of the providers in the chain");
        DefaultCredentialsChain chain = new DefaultCredentialsChain(() -> {
            throw noCredentials;
        });

        assertThatThrownBy(chain::resolveCredentials)
                .isInstanceOf(SdkClientException.class)
                .hasMessageContaining("storage.s3.anonymous=true")
                .hasCause(noCredentials);
    }

    @Test
    void credentialsFoundByTheChainPassThrough() {
        AwsBasicCredentials credentials = AwsBasicCredentials.create("key", "secret");
        DefaultCredentialsChain chain = new DefaultCredentialsChain(() -> credentials);

        assertThat(chain.resolveCredentials()).isSameAs(credentials);
    }
}
