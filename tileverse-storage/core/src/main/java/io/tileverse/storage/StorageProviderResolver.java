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

import io.tileverse.storage.spi.StorageProvider;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Resolves the {@link StorageProvider} of a {@link StorageConfig} from the config alone, with no request.
 *
 * <ol>
 *   <li>The provider named by {@code config.providerId()} wins.
 *   <li>With none named, the available providers answering {@code canProcess(config)} are the candidates. The URI
 *       scheme decides among the built-in providers, and an {@code http(s)} URI belongs to the HTTP provider.
 *   <li>Several candidates are settled by the lowest {@link StorageProvider#getOrder()}; equal orders are an error.
 * </ol>
 *
 * <p>This is package-private internal machinery; the public entry point is {@link StorageFactory}.
 */
final class StorageProviderResolver {

    private StorageProviderResolver() {}

    static StorageProvider findBestProvider(StorageConfig config) {
        requireNonNull(config.baseUri(), "StorageConfig.baseUri() is required");
        Optional<String> namedProvider = config.providerId();
        if (namedProvider.isPresent()) {
            return StorageProvider.getProvider(namedProvider.orElseThrow(), true);
        }
        return selectByUri(config, StorageProvider.getAvailableProviders());
    }

    /** Selects among {@code availableProviders} for a config naming no provider. */
    static StorageProvider selectByUri(StorageConfig config, List<StorageProvider> availableProviders) {
        List<StorageProvider> candidates =
                availableProviders.stream().filter(p -> p.canProcess(config)).toList();

        return switch (candidates.size()) {
            case 0 -> throw new IllegalStateException("No suitable provider found for URI: " + config.baseUri());
            case 1 -> candidates.get(0);
            default -> resolveByPriority(candidates);
        };
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
}
