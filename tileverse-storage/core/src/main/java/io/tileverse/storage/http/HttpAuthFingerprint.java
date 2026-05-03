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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Stable identity of an {@link HttpAuthentication} configuration, suitable for embedding in an HTTP-client cache key.
 *
 * <p>The fingerprint captures only the credential identity: two authentications that would authenticate the same
 * principal against the same server compare equal. It deliberately excludes any live state such as cached server
 * challenges or nonce counters, so a {@link DigestAuthentication} that has issued requests is fingerprint-equal to a
 * fresh instance built with the same username and password.
 *
 * <p>The {@code credentialsHash} is an opaque SHA-256 hex digest derived from a per-auth canonical form. Logging or
 * exposing the fingerprint does not leak the underlying secrets.
 */
public record HttpAuthFingerprint(String type, String credentialsHash) {

    /** Fingerprint of {@link HttpAuthentication#NONE}. */
    public static final HttpAuthFingerprint NONE = new HttpAuthFingerprint("none", "");

    public HttpAuthFingerprint {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(credentialsHash, "credentialsHash");
    }

    /**
     * Computes a SHA-256 hex digest over the supplied canonical form. Use as the {@code credentialsHash} component when
     * constructing a fingerprint; the canonical form must include the auth type discriminator and every field that
     * distinguishes principals (e.g. {@code "basic:" + username + ":" + password}), so different auth types cannot
     * collide.
     */
    public static String sha256Hex(String canonical) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
