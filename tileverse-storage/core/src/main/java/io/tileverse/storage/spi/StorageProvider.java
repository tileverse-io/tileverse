/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.storage.spi;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Stream;

/**
 * Service Provider Interface (SPI) for opening {@link Storage} instances. Implementations are discovered at runtime via
 * {@link ServiceLoader} and selected by {@link io.tileverse.storage.StorageFactory} based on the URI (and, for
 * ambiguous {@code http(s)} URIs, a HEAD-probe disambiguation step).
 *
 * <p>To produce a {@link RangeReader} for a single object, open a {@link Storage} rooted at the object's container with
 * {@link #createStorage(StorageConfig)} and call {@link Storage#openRangeReader(String)} with the relative key (or
 * {@link Storage#openRangeReader(URI)} with the absolute URI; the Storage validates and relativizes).
 */
public interface StorageProvider {

    /**
     * Returns the unique identifier for this provider.
     *
     * @return The unique ID.
     */
    String getId();

    /**
     * Returns a human-readable description of this provider.
     *
     * @return The description.
     */
    String getDescription();

    /**
     * Checks if this provider is available in the current environment. For example, a provider might check for the
     * presence of a specific library or a configuration flag.
     *
     * @return {@code true} if available, {@code false} otherwise.
     */
    boolean isAvailable();

    /**
     * Returns a list of configuration parameters supported by this provider.
     *
     * @return A list of {@link StorageParameter}s.
     */
    List<StorageParameter<?>> getParameters();

    /**
     * Returns the default configuration for this provider, populated with default values.
     *
     * @return The default {@link StorageConfig}.
     */
    default StorageConfig getDefaultConfig() {
        return StorageConfig.withDefaults(getParameters());
    }

    /**
     * Performs a fast, static check to see if this provider can likely handle the given config. This check should be
     * based on URI schemes and hostname patterns only, without I/O.
     *
     * @param config The configuration to check.
     * @return {@code true} if this provider can likely handle the config, {@code false} otherwise.
     */
    boolean canProcess(StorageConfig config);

    /**
     * Convenience helper for {@link #canProcess(StorageConfig)} implementations: returns {@code true} when the config
     * does not name a different provider AND the config's {@link StorageConfig#baseUri() baseUri} has one of the
     * supplied schemes (a {@code null} element in {@code acceptedUriSchemes} matches a scheme-less URI like a bare
     * filesystem path).
     *
     * @param config the configuration to check; its {@link StorageConfig#baseUri()} must not be {@code null}
     * @param acceptedUriSchemes URI schemes this provider claims (e.g. {@code "file"}, {@code "http"}, {@code "https"})
     * @return {@code true} if the config matches this provider's id (when set) and one of the accepted schemes
     * @throws NullPointerException if {@code config} or {@code config.baseUri()} is {@code null}
     */
    default boolean matches(StorageConfig config, String... acceptedUriSchemes) {
        Objects.requireNonNull(config, "config parameter is null");
        Objects.requireNonNull(config.baseUri(), "config baseUri is null");
        if (config.providerId().isPresent()
                && !config.providerId().orElseThrow().equals(getId())) {
            return false;
        }
        // may be null; null in acceptedUriSchemes matches scheme-less URIs (bare paths).
        final String scheme = config.baseUri().getScheme();
        return Arrays.asList(acceptedUriSchemes).contains(scheme);
    }

    /**
     * Performs a more definitive check by inspecting HTTP headers from a HEAD request. This method is only called for
     * ambiguous http(s) URIs as a final disambiguation step.
     *
     * @param uri the URI of the returned headers, can be used to disambiguate based on well-known host names
     * @param headers The HTTP headers from a HEAD request to the resource URI.
     * @return {@code true} if the headers confirm this provider can handle the resource.
     */
    default boolean canProcessHeaders(URI uri, Map<String, List<String>> headers) {
        return false; // Opt-in: only cloud providers need to implement this.
    }

    /**
     * Gets the order value of this provider. Lower values have higher priority. The default priority is 0.
     *
     * @return The order value.
     */
    default int getOrder() {
        return 0;
    }

    /**
     * Open a {@link Storage} rooted at {@code config}'s URI. Implementations consult the {@link StorageConfig} for
     * credentials, region, endpoint overrides, and any other backend-specific options. The returned {@code Storage}
     * owns the underlying SDK client and remains usable until {@link Storage#close() closed}; per-key
     * {@link RangeReader RangeReaders} obtained from it inherit those settings.
     */
    Storage createStorage(StorageConfig config);

    /**
     * Checks if a feature is enabled via a system property or environment variable. The check is case-sensitive. The
     * property is checked first, then the environment variable. If neither is set, it defaults to {@code true}.
     *
     * @param key The key for the system property/environment variable.
     * @return {@code true} if enabled, {@code false} otherwise.
     */
    static boolean isEnabled(String key) {
        String enabled = System.getProperty(key);
        if (enabled == null) {
            enabled = System.getenv(key);
        }
        return enabled == null || Boolean.parseBoolean(enabled);
    }

    /**
     * Finds all {@link StorageProvider} implementations using the {@link ServiceLoader}, whether they're
     * {@link StorageProvider#isAvailable() available} or not.
     *
     * @return A stream of installed providers.
     */
    static Stream<StorageProvider> findProviders() {
        ServiceLoader<StorageProvider> loader = ServiceLoader.load(StorageProvider.class);
        return loader.stream().map(Provider::get);
    }

    /**
     * Finds all {@link StorageProvider#isAvailable() available} {@link StorageProvider} implementations using the
     * {@link ServiceLoader}.
     *
     * @return A stream of installed providers.
     */
    static Stream<StorageProvider> findAvailableProviders() {
        return findProviders().filter(StorageProvider::isAvailable);
    }

    /**
     * Returns all {@link StorageProvider}s registered through the standard Java SPI mechanism.
     *
     * @return A list of all registered providers.
     */
    static List<StorageProvider> getProviders() {
        return findProviders().toList();
    }

    /**
     * Returns all {@link StorageProvider}s registered through the standard Java SPI mechanism that are
     * {@link StorageProvider#isAvailable() available}.
     *
     * @return A list of available providers.
     */
    static List<StorageProvider> getAvailableProviders() {
        return findAvailableProviders().toList();
    }

    /**
     * Finds a specific {@link StorageProvider} by its ID.
     *
     * @param providerId The ID of the provider to find.
     * @return An {@link Optional} containing the provider if found, otherwise empty.
     */
    static Optional<StorageProvider> findProvider(String providerId) {
        return findProviders()
                .filter(p -> p.getId().equalsIgnoreCase(providerId))
                .findFirst();
    }

    /**
     * Retrieves a specific {@link StorageProvider} by its ID, with an option to check for availability.
     *
     * @param providerId The ID of the provider to retrieve.
     * @param available If {@code true}, the method will throw an exception if the provider is not available.
     * @return The requested {@link StorageProvider}.
     * @throws IllegalStateException if the provider is not found, or if {@code available} is true and the provider is
     *     not available.
     */
    static StorageProvider getProvider(String providerId, boolean available) {
        StorageProvider provider = findProviders()
                .filter(p -> p.getId().equalsIgnoreCase(providerId))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalStateException("The specified StorageProvider is not found: " + providerId));

        if (available && !provider.isAvailable()) {
            throw new IllegalStateException("The specified StorageProvider is not available: " + providerId);
        }
        return provider;
    }
}
