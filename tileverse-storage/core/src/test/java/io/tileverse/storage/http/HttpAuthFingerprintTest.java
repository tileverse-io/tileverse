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
package io.tileverse.storage.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Verifies that every {@link HttpAuthentication} implementation produces a stable, equality-comparable
 * {@link HttpAuthFingerprint} suitable for embedding in a cache key.
 *
 * <p>Stability matters most for {@link DigestAuthentication}, whose challenge cache is mutated by
 * {@link DigestAuthentication#authenticate} but must NOT influence the fingerprint.
 */
class HttpAuthFingerprintTest {

    @Test
    void noneFingerprintIsConstant() {
        assertThat(HttpAuthentication.NONE.fingerprint()).isEqualTo(HttpAuthFingerprint.NONE);
    }

    @Test
    void basicFingerprintIsDeterministic() {
        HttpAuthFingerprint a = new BasicAuthentication("alice", "s3cret").fingerprint();
        HttpAuthFingerprint b = new BasicAuthentication("alice", "s3cret").fingerprint();
        assertThat(a).isEqualTo(b);
        assertThat(a.type()).isEqualTo("basic");
        assertThat(a.credentialsHash()).hasSize(64); // sha256 hex
    }

    @Test
    void basicFingerprintDistinguishesPrincipals() {
        HttpAuthFingerprint a = new BasicAuthentication("alice", "s3cret").fingerprint();
        HttpAuthFingerprint b = new BasicAuthentication("bob", "s3cret").fingerprint();
        HttpAuthFingerprint c = new BasicAuthentication("alice", "different").fingerprint();
        assertThat(a).isNotEqualTo(b).isNotEqualTo(c);
    }

    @Test
    void bearerFingerprintIsDeterministic() {
        HttpAuthFingerprint a = new BearerTokenAuthentication("eyJabc").fingerprint();
        HttpAuthFingerprint b = new BearerTokenAuthentication("eyJabc").fingerprint();
        assertThat(a).isEqualTo(b);
        assertThat(a.type()).isEqualTo("bearer");
    }

    @Test
    void bearerFingerprintDistinguishesTokens() {
        HttpAuthFingerprint a = new BearerTokenAuthentication("token-a").fingerprint();
        HttpAuthFingerprint b = new BearerTokenAuthentication("token-b").fingerprint();
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void apiKeyFingerprintIsDeterministicAndCoversPrefix() {
        HttpAuthFingerprint a = new ApiKeyAuthentication("X-API-Key", "abc123", "Bearer ").fingerprint();
        HttpAuthFingerprint b = new ApiKeyAuthentication("X-API-Key", "abc123", "Bearer ").fingerprint();
        HttpAuthFingerprint differentPrefix = new ApiKeyAuthentication("X-API-Key", "abc123", "Token ").fingerprint();
        HttpAuthFingerprint differentHeader =
                new ApiKeyAuthentication("X-Other-Header", "abc123", "Bearer ").fingerprint();
        assertThat(a).isEqualTo(b);
        assertThat(a).isNotEqualTo(differentPrefix);
        assertThat(a).isNotEqualTo(differentHeader);
    }

    @Test
    void apiKeyTwoArgConvenienceMatchesEmptyPrefix() {
        HttpAuthFingerprint twoArg = new ApiKeyAuthentication("X-API-Key", "abc123").fingerprint();
        HttpAuthFingerprint threeArg = new ApiKeyAuthentication("X-API-Key", "abc123", "").fingerprint();
        assertThat(twoArg).isEqualTo(threeArg);
    }

    @Test
    void customHeaderFingerprintIsOrderIndependent() {
        Map<String, String> insertionOrderA = new LinkedHashMap<>();
        insertionOrderA.put("X-Tenant", "acme");
        insertionOrderA.put("X-Auth", "key123");
        Map<String, String> insertionOrderB = new LinkedHashMap<>();
        insertionOrderB.put("X-Auth", "key123");
        insertionOrderB.put("X-Tenant", "acme");
        HttpAuthFingerprint a = new CustomHeaderAuthentication(insertionOrderA).fingerprint();
        HttpAuthFingerprint b = new CustomHeaderAuthentication(insertionOrderB).fingerprint();
        assertThat(a).isEqualTo(b);
    }

    @Test
    void customHeaderFingerprintDistinguishesEmbeddedSeparators() {
        // Without length-prefixing, ("foo:bar", "baz") and ("foo", "bar:baz") could collide on a naive canonical form.
        HttpAuthFingerprint a = new CustomHeaderAuthentication(Map.of("foo:bar", "baz")).fingerprint();
        HttpAuthFingerprint b = new CustomHeaderAuthentication(Map.of("foo", "bar:baz")).fingerprint();
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void digestFingerprintIgnoresLiveChallengeState() {
        DigestAuthentication fresh = new DigestAuthentication("alice", "s3cret");
        DigestAuthentication used = new DigestAuthentication("alice", "s3cret");
        // Mutate the second instance: invoking authenticate() against an unreachable URI populates the challenge cache
        // (or at least exercises the warning-and-skip path); either way it advances the nonce counter.
        HttpClient client = HttpClient.newHttpClient();
        try {
            HttpRequest.Builder builder =
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:1/")).GET();
            used.authenticate(client, builder);
        } finally {
            // Java 21+: HttpClient is AutoCloseable
            if (client instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
            }
        }
        assertThat(fresh.fingerprint()).isEqualTo(used.fingerprint());
        assertThat(fresh.fingerprint().type()).isEqualTo("digest");
    }

    @Test
    void digestFingerprintDistinguishesCredentials() {
        HttpAuthFingerprint alice = new DigestAuthentication("alice", "s3cret").fingerprint();
        HttpAuthFingerprint bob = new DigestAuthentication("bob", "s3cret").fingerprint();
        HttpAuthFingerprint differentPassword = new DigestAuthentication("alice", "other").fingerprint();
        assertThat(alice).isNotEqualTo(bob).isNotEqualTo(differentPassword);
    }

    @Test
    void typesDoNotCollide() {
        // Two auths happening to share the same secret-fragment shape must still produce distinct fingerprints because
        // the canonical form is type-prefixed.
        HttpAuthFingerprint basic = new BasicAuthentication("user", "pass").fingerprint();
        HttpAuthFingerprint digest = new DigestAuthentication("user", "pass").fingerprint();
        assertThat(basic).isNotEqualTo(digest);
        assertThat(basic.type()).isNotEqualTo(digest.type());
    }
}
