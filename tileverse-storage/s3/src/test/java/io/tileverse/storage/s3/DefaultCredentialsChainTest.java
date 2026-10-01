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
