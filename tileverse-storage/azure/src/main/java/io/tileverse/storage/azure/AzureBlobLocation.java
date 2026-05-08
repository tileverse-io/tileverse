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
package io.tileverse.storage.azure;

import com.azure.storage.blob.BlobUrlParts;
import java.net.URI;

/**
 * Parsed Azure root URI for a Storage instance: endpoint + account + container + prefix. Supports {@code https://} URIs
 * to {@code blob.core.windows.net} or {@code dfs.core.windows.net}, {@code abfs://} / {@code abfss://} URIs for
 * DataLake Gen2, and the short-form {@code az://<account>/<container>/<path>} convention used by tools like DuckDB and
 * various data engineering libraries.
 *
 * <p>The prefix never starts with '/' and ends with '/' if non-empty.
 */
record AzureBlobLocation(String endpoint, String accountName, String container, String prefix) {

    static AzureBlobLocation parse(URI uri) {
        if (uri == null) {
            throw new IllegalArgumentException("URI is required");
        }
        String scheme = uri.getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException("URI is missing a scheme: " + uri);
        }
        String s = scheme.toLowerCase();
        if (s.equals("abfs") || s.equals("abfss")) {
            return parseAbfs(uri);
        }
        if (s.equals("http") || s.equals("https")) {
            return parseHttp(uri);
        }
        if (s.equals("az")) {
            return parseAz(uri);
        }
        throw new IllegalArgumentException("Unsupported Azure URI scheme: " + scheme);
    }

    /**
     * Parses {@code az://<account>/<container>/<path>} into the canonical Blob Storage form. The account is taken from
     * the URI host, the container is the first path segment, and any remaining segments form the prefix.
     */
    private static AzureBlobLocation parseAz(URI uri) {
        String accountName = uri.getHost();
        if (accountName == null || accountName.isBlank()) {
            throw new IllegalArgumentException("az:// URI must have an account name as host: " + uri);
        }
        String rawPath = uri.getPath() == null ? "" : uri.getPath();
        String path = rawPath;
        while (path.startsWith("/")) path = path.substring(1);
        int slashIdx = path.indexOf('/');
        String container;
        String prefix;
        if (slashIdx < 0) {
            container = path;
            prefix = "";
        } else {
            container = path.substring(0, slashIdx);
            prefix = path.substring(slashIdx + 1);
        }
        if (container.isEmpty()) {
            throw new IllegalArgumentException("az:// URI must have a container after the account: " + uri);
        }
        String endpoint = "https://" + accountName + ".blob.core.windows.net";
        return new AzureBlobLocation(endpoint, accountName, container, normalizePrefix(prefix));
    }

    private static AzureBlobLocation parseAbfs(URI uri) {
        String userInfo = uri.getUserInfo();
        String host = uri.getHost();
        if (userInfo == null || host == null || !host.contains(".dfs.core.")) {
            throw new IllegalArgumentException("Invalid abfs URI: " + uri);
        }
        String container = userInfo;
        String accountName = host.substring(0, host.indexOf('.'));
        String endpoint = "https://" + host;
        String path = uri.getPath() == null ? "" : uri.getPath();
        return new AzureBlobLocation(endpoint, accountName, container, normalizePrefix(path));
    }

    private static AzureBlobLocation parseHttp(URI uri) {
        BlobUrlParts parts = BlobUrlParts.parse(uri.toString());
        String container = parts.getBlobContainerName();
        if (container == null) {
            throw new IllegalArgumentException("Azure URI is missing a container: " + uri);
        }
        String accountName = parts.getAccountName();
        // Use authority (host[:port]) so non-default ports (e.g. Azurite emulator) survive.
        String endpoint = uri.getScheme() + "://" + uri.getAuthority();
        // For path-style URLs (Azurite-style http://host:port/<account>/<container>/...) the
        // emulator account name lives in the path; the BlobServiceClient endpoint must include it.
        if (accountName != null
                && !accountName.isBlank()
                && uri.getHost() != null
                && !uri.getHost().contains(accountName)) {
            endpoint = endpoint + "/" + accountName;
        }
        String prefix = parts.getBlobName() == null ? "" : parts.getBlobName();
        return new AzureBlobLocation(endpoint, accountName, container, normalizePrefix(prefix));
    }

    private static String normalizePrefix(String s) {
        if (s == null) {
            return "";
        }
        String p = s;
        while (p.startsWith("/")) p = p.substring(1);
        if (!p.isEmpty() && !p.endsWith("/")) {
            p = p + "/";
        }
        return p;
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
