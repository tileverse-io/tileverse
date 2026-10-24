/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.gcs;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * Parsed GCS root URI for a Storage instance: bucket plus an optional key prefix. Supports {@code gs://},
 * {@code https://storage.googleapis.com/...}, {@code https://storage.cloud.google.com/...}, and emulator API URIs
 * {@code http(s)://host:port/storage/v1/b/<bucket>/o/<key>}.
 *
 * <p>The prefix never starts with '/' and ends with '/' if non-empty.
 */
record SdkStorageLocation(String bucket, String prefix) {

    static SdkStorageLocation parse(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("URI is required");
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
        String bucket;
        String key;
        final String path = Optional.ofNullable(uri.getPath()).orElse("");
        if (scheme.equals("gs")) {
            bucket = Objects.requireNonNull(uri.getHost(), "host");
            key = path;
        } else if (scheme.equals("http") || scheme.equals("https")) {
            String emulatorPrefix = "/storage/v1/b/";
            if (path.startsWith(emulatorPrefix)) {
                String rest = path.substring(emulatorPrefix.length());
                int oIdx = rest.indexOf("/o/");
                if (oIdx >= 0) {
                    bucket = rest.substring(0, oIdx);
                    key = rest.substring(oIdx + 3);
                } else {
                    bucket = rest.endsWith("/") ? rest.substring(0, rest.length() - 1) : rest;
                    key = "";
                }
            } else if ("storage.googleapis.com".equals(uri.getHost())
                    || "storage.cloud.google.com".equals(uri.getHost())) {
                String stripped = path.startsWith("/") ? path.substring(1) : path;
                int slash = stripped.indexOf('/');
                if (slash < 0) {
                    bucket = stripped;
                    key = "";
                } else {
                    bucket = stripped.substring(0, slash);
                    key = stripped.substring(slash + 1);
                }
            } else {
                throw new IllegalArgumentException("Unsupported GCS HTTP URI: " + uri);
            }
        } else {
            throw new IllegalArgumentException("Unsupported GCS URI scheme: " + scheme);
        }
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("GCS URI is missing a bucket: " + uri);
        }
        while (key.startsWith("/")) key = key.substring(1);
        if (!key.isEmpty() && !key.endsWith("/")) {
            key = key + "/";
        }
        return new SdkStorageLocation(bucket, key);
    }

    String resolve(String relativeKey) {
        io.tileverse.storage.Storage.requireSafeKey(relativeKey);
        return prefix + relativeKey;
    }

    String relativize(String fullKey) {
        if (prefix.isEmpty()) {
            return fullKey;
        }
        if (!fullKey.startsWith(prefix)) {
            throw new IllegalArgumentException("Key '" + fullKey + "' does not start with prefix '" + prefix + "'");
        }
        return fullKey.substring(prefix.length());
    }
}
