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

import io.tileverse.storage.StorageConfig;
import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Verifies {@link S3StorageProvider#keyFor(StorageConfig)} resolution precedence: region, endpoint override, and
 * credential parameters. Replaces the reflection-based {@code testResolveCredentialsProviderPrecedence} /
 * {@code testResolveRegionUsesExplicitAndParsedValues} cases that lived in the now-removed
 * {@code S3RangeReader.Builder}.
 */
class S3StorageProviderConfigTest {

    @Test
    void regionResolution_explicitParameterWins() {
        StorageConfig config = new StorageConfig()
                .baseUri(URI.create("https://my-bucket.s3.us-west-2.amazonaws.com/file.txt"))
                .setParameter(S3StorageProvider.S3_REGION, "us-east-1");

        assertThat(S3StorageProvider.keyFor(config).region()).isEqualTo("us-east-1");
    }

    @Test
    void regionResolution_fallsBackToUriParsedRegion() {
        StorageConfig config =
                new StorageConfig().baseUri(URI.create("https://my-bucket.s3.us-west-2.amazonaws.com/file.txt"));

        assertThat(S3StorageProvider.keyFor(config).region()).isEqualTo("us-west-2");
    }

    @Test
    void regionResolution_fallsBackToUsEast1WhenAbsent() {
        StorageConfig config = new StorageConfig().baseUri(URI.create("s3://my-bucket/file.txt"));

        assertThat(S3StorageProvider.keyFor(config).region()).isEqualTo("us-east-1");
    }

    @Test
    void regionResolution_blankExplicitRegionFallsBack() {
        StorageConfig config = new StorageConfig()
                .baseUri(URI.create("https://my-bucket.s3.us-west-2.amazonaws.com/file.txt"))
                .setParameter(S3StorageProvider.S3_REGION, "   ");

        assertThat(S3StorageProvider.keyFor(config).region()).isEqualTo("us-west-2");
    }

    @Test
    void endpointOverride_extractedFromCustomHost() {
        StorageConfig config = new StorageConfig().baseUri(URI.create("http://localhost:9000/my-bucket/file.txt"));

        assertThat(S3StorageProvider.keyFor(config).endpointOverride()).hasValue(URI.create("http://localhost:9000"));
    }

    @Test
    void endpointOverride_emptyForCanonicalAwsUri() {
        StorageConfig config = new StorageConfig().baseUri(URI.create("s3://my-bucket/file.txt"));

        assertThat(S3StorageProvider.keyFor(config).endpointOverride()).isEmpty();
    }

    @Test
    void credentials_staticAccessAndSecretKeyAreCarriedOnTheKey() {
        StorageConfig config = new StorageConfig()
                .baseUri(URI.create("s3://my-bucket/file.txt"))
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-static")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "secret");

        S3ClientCache.Key key = S3StorageProvider.keyFor(config);
        assertThat(key.accessKeyId()).hasValue("AKIA-static");
        assertThat(key.secretAccessKey()).hasValue("secret");
    }

    @Test
    void credentials_profileNameCarried() {
        StorageConfig config = new StorageConfig()
                .baseUri(URI.create("s3://my-bucket/file.txt"))
                .setParameter(S3StorageProvider.S3_DEFAULT_CREDENTIALS_PROFILE, "production");

        assertThat(S3StorageProvider.keyFor(config).profile()).hasValue("production");
    }

    @Test
    void credentials_emptyWhenNoCredentialsConfigured() {
        StorageConfig config = new StorageConfig().baseUri(URI.create("s3://my-bucket/file.txt"));

        S3ClientCache.Key key = S3StorageProvider.keyFor(config);
        assertThat(key.accessKeyId()).isEmpty();
        assertThat(key.secretAccessKey()).isEmpty();
        assertThat(key.profile()).isEmpty();
    }

    @Test
    void forcePathStyle_defaultsToFalse() {
        StorageConfig config = new StorageConfig().baseUri(URI.create("s3://my-bucket/file.txt"));

        assertThat(S3StorageProvider.keyFor(config).forcePathStyle()).isFalse();
    }

    @Test
    void forcePathStyle_explicitTrue() {
        StorageConfig config = new StorageConfig()
                .baseUri(URI.create("https://minio.example.com/my-bucket/file.txt"))
                .setParameter(S3StorageProvider.S3_FORCE_PATH_STYLE, true);

        assertThat(S3StorageProvider.keyFor(config).forcePathStyle()).isTrue();
    }

    @Test
    void sameKeyIsReturnedForEquivalentConfigs() {
        StorageConfig a = new StorageConfig()
                .baseUri(URI.create("s3://my-bucket/file.txt"))
                .setParameter(S3StorageProvider.S3_REGION, "us-west-2")
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-X")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "secret");
        StorageConfig b = new StorageConfig()
                .baseUri(URI.create("s3://my-bucket/different-key.txt"))
                .setParameter(S3StorageProvider.S3_REGION, "us-west-2")
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-X")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "secret");

        // Cache discriminator is independent of the per-object key (different files in same bucket
        // share the same SDK client, by design).
        assertThat(S3StorageProvider.keyFor(a)).isEqualTo(S3StorageProvider.keyFor(b));
    }

    @Test
    void differentRegionsProduceDifferentCacheKeys() {
        StorageConfig a = new StorageConfig()
                .baseUri(URI.create("s3://my-bucket/file.txt"))
                .setParameter(S3StorageProvider.S3_REGION, "us-west-2");
        StorageConfig b = new StorageConfig()
                .baseUri(URI.create("s3://my-bucket/file.txt"))
                .setParameter(S3StorageProvider.S3_REGION, "eu-central-1");

        assertThat(S3StorageProvider.keyFor(a)).isNotEqualTo(S3StorageProvider.keyFor(b));
    }

    @Test
    void differentAccessKeysProduceDifferentCacheKeys() {
        URI uri = URI.create("s3://my-bucket/file.txt");
        StorageConfig a = new StorageConfig()
                .baseUri(uri)
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-A")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "s");
        StorageConfig b = new StorageConfig()
                .baseUri(uri)
                .setParameter(S3StorageProvider.S3_AWS_ACCESS_KEY_ID, "AKIA-B")
                .setParameter(S3StorageProvider.S3_AWS_SECRET_ACCESS_KEY, "s");

        S3ClientCache.Key keyA = S3StorageProvider.keyFor(a);
        S3ClientCache.Key keyB = S3StorageProvider.keyFor(b);
        assertThat(keyA.accessKeyId()).isEqualTo(Optional.of("AKIA-A"));
        assertThat(keyB.accessKeyId()).isEqualTo(Optional.of("AKIA-B"));
        assertThat(keyA).isNotEqualTo(keyB);
    }
}
