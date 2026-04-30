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
package io.tileverse.storage;

import static java.util.Objects.requireNonNull;

import io.tileverse.storage.http.HttpStorageProvider;
import io.tileverse.storage.spi.StorageConfig;
import io.tileverse.storage.spi.StorageProvider;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the best {@link StorageProvider} for a given {@link StorageConfig}.
 *
 * <p>Selection rules (in order):
 *
 * <ol>
 *   <li>Explicit {@code config.providerId()} wins.
 *   <li>Single available provider whose {@code canProcess(config)} is true wins.
 *   <li>For {@code http(s)://} URIs with multiple candidates: probe the URI with a HEAD request and call
 *       {@code canProcessHeaders(uri, headers)} on each candidate; if exactly one matches, use it; otherwise fall back
 *       to the generic HTTP provider.
 *   <li>For non-HTTP schemes with multiple candidates: pick the one with the lowest {@link StorageProvider#getOrder()}
 *       (highest priority).
 * </ol>
 *
 * <p>This is package-private internal machinery; the public entry point is {@link StorageFactory}.
 */
@Slf4j
final class StorageProviderResolver {

    private static final HttpClient HTTP_CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

    private StorageProviderResolver() {}

    static StorageProvider findBestProvider(StorageConfig config) {
        URI uri = requireNonNull(config.uri(), "StorageConfig.uri() is required");

        if (config.providerId().isPresent()) {
            return StorageProvider.getProvider(config.providerId().orElseThrow(), true);
        }

        List<StorageProvider> candidates = StorageProvider.getAvailableProviders().stream()
                .filter(p -> p.canProcess(config))
                .toList();

        return switch (candidates.size()) {
            case 0 -> throw new IllegalStateException("No suitable provider found for URI: " + uri);
            case 1 -> candidates.get(0);
            default -> disambiguate(uri, candidates);
        };
    }

    private static StorageProvider disambiguate(URI uri, List<StorageProvider> candidates) {
        String scheme = uri.getScheme();
        boolean isHttp = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
        return isHttp ? disambiguateHttpUri(uri, candidates) : resolveByPriority(candidates);
    }

    private static StorageProvider disambiguateHttpUri(URI uri, List<StorageProvider> httpCandidates) {
        // Prefer specific cloud providers over the generic HTTP provider.
        List<StorageProvider> specificCandidates = httpCandidates.stream()
                .filter(p -> !(p instanceof HttpStorageProvider))
                .toList();

        try {
            Map<String, List<String>> headers = probeUriHeaders(uri);
            List<StorageProvider> probedCandidates = specificCandidates.stream()
                    .filter(p -> p.canProcessHeaders(uri, headers))
                    .toList();

            if (probedCandidates.isEmpty()) {
                return httpCandidates.stream()
                        .filter(HttpStorageProvider.class::isInstance)
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("HttpRangeReaderProvider not found"));
            }
            if (probedCandidates.size() == 1) {
                return probedCandidates.get(0);
            }
            specificCandidates = probedCandidates;
        } catch (Exception e) {
            log.warn("HEAD request probe failed for {}: {}", uri, e.getMessage(), e);
        }

        return resolveByPriority(specificCandidates);
    }

    private static StorageProvider resolveByPriority(List<StorageProvider> candidates) {
        int highestPriority = candidates.stream()
                .mapToInt(StorageProvider::getOrder)
                .min()
                .orElseThrow(() -> new IllegalStateException("No candidates to resolve by priority"));
        List<StorageProvider> bestCandidates =
                candidates.stream().filter(p -> p.getOrder() == highestPriority).toList();

        if (bestCandidates.size() > 1) {
            String conflictingIds =
                    bestCandidates.stream().map(StorageProvider::getId).collect(Collectors.joining(", "));
            throw new IllegalStateException(
                    "URI ambiguity detected. Multiple providers matched with the same priority (" + highestPriority
                            + "): [" + conflictingIds + "]. "
                            + "Please specify a provider ID in the StorageConfig to resolve this ambiguity.");
        }
        return bestCandidates.get(0);
    }

    private static Map<String, List<String>> probeUriHeaders(URI uri) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(3))
                .build();
        HttpResponse<Void> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
        return response.headers().map();
    }
}
