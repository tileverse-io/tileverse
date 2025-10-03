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
package io.tileverse.rangereader.spi;

import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Stream;

/**
 * Service Provider Interface (SPI) for creating {@link RangeReader} instances.
 * Implementations of this interface can be discovered at runtime using {@link ServiceLoader}.
 */
public interface RangeReaderProvider {

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
     * Checks if this provider is available in the current environment.
     * For example, a provider might check for the presence of a specific library or a configuration flag.
     *
     * @return {@code true} if available, {@code false} otherwise.
     */
    boolean isAvailable();

    /**
     * Returns a list of configuration parameters supported by this provider.
     *
     * @return A list of {@link RangeReaderParameter}s.
     */
    List<RangeReaderParameter<?>> getParameters();

    /**
     * Returns the default configuration for this provider, populated with default values.
     *
     * @return The default {@link RangeReaderConfig}.
     */
    default RangeReaderConfig getDefaultConfig() {
        return RangeReaderConfig.withDefaults(getParameters());
    }

    /**
     * Performs a fast, static check to see if this provider can likely handle the given config.
     * This check should be based on URI schemes and hostname patterns only, without I/O.
     *
     * @param config The configuration to check.
     * @return {@code true} if this provider can likely handle the config, {@code false} otherwise.
     */
    boolean canProcess(RangeReaderConfig config);

    /**
     * Performs a more definitive check by inspecting HTTP headers from a HEAD request.
     * This method is only called for ambiguous http(s) URIs as a final disambiguation step.
     *
     * @param uri the URI of the returned headers, can be used to disambiguate based on well-known host names
     * @param headers The HTTP headers from a HEAD request to the resource URI.
     * @return {@code true} if the headers confirm this provider can handle the resource.
     */
    default boolean canProcessHeaders(URI uri, Map<String, List<String>> headers) {
        return false; // Opt-in: only cloud providers need to implement this.
    }

    /**
     * Gets the order value of this provider. Lower values have higher priority.
     * The default priority is 0.
     *
     * @return The order value.
     */
    default int getOrder() {
        return 0;
    }

    /**
     * Creates a new {@link RangeReader} instance for the given URI using default configuration.
     *
     * @param uri The URI of the resource to read.
     * @return A new {@link RangeReader} instance.
     * @throws IOException If an I/O error occurs during reader creation.
     */
    default RangeReader create(URI uri) throws IOException {
        return create(getDefaultConfig().uri(uri));
    }

    /**
     * Creates a new {@link RangeReader} instance with the specified configuration.
     *
     * @param opts The configuration for the {@link RangeReader}.
     * @return A new {@link RangeReader} instance.
     * @throws IOException If an I/O error occurs during reader creation.
     */
    RangeReader create(RangeReaderConfig opts) throws IOException;

    /**
     * Checks if a feature is enabled via a system property or environment variable.
     * The check is case-sensitive. The property is checked first, then the environment variable.
     * If neither is set, it defaults to {@code true}.
     *
     * @param key The key for the system property/environment variable.
     * @return {@code true} if enabled, {@code false} otherwise.
     */
    static boolean isEnabled(String key) {
        String enabled = System.getProperty(key);
        if (enabled == null) {
            enabled = System.getenv(key);
        }
        return enabled == null ? true : Boolean.parseBoolean(enabled);
    }

    /**
     * Finds all available {@link RangeReaderProvider} implementations using the {@link ServiceLoader}.
     *
     * @return A stream of available providers.
     */
    static Stream<RangeReaderProvider> findProviders() {
        ServiceLoader<RangeReaderProvider> loader = ServiceLoader.load(RangeReaderProvider.class);
        return loader.stream().map(Provider::get);
    }

    /**
     * Returns all {@link RangeReaderProvider}s registered through the standard Java SPI mechanism.
     *
     * @return A list of all registered providers.
     */
    static List<RangeReaderProvider> getProviders() {
        return findProviders().toList();
    }

    /**
     * Returns all {@link RangeReaderProvider}s registered through the standard Java SPI mechanism
     * that are {@link RangeReaderProvider#isAvailable() available}.
     *
     * @return A list of available providers.
     */
    static List<RangeReaderProvider> getAvailableProviders() {
        return findProviders().filter(RangeReaderProvider::isAvailable).toList();
    }

    /**
     * Finds a specific {@link RangeReaderProvider} by its ID.
     *
     * @param providerId The ID of the provider to find.
     * @return An {@link Optional} containing the provider if found, otherwise empty.
     */
    static Optional<RangeReaderProvider> findProvider(String providerId) {
        return findProviders()
                .filter(p -> p.getId().equalsIgnoreCase(providerId))
                .findFirst();
    }

    /**
     * Retrieves a specific {@link RangeReaderProvider} by its ID, with an option to check for availability.
     *
     * @param providerId The ID of the provider to retrieve.
     * @param available  If {@code true}, the method will throw an exception if the provider is not available.
     * @return The requested {@link RangeReaderProvider}.
     * @throws IllegalStateException if the provider is not found, or if {@code available} is true and the provider is not available.
     */
    static RangeReaderProvider getProvider(String providerId, boolean available) {
        RangeReaderProvider provider = findProviders()
                .filter(p -> p.getId().equalsIgnoreCase(providerId))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalStateException("The specified RangeReaderProvider is not found: " + providerId));

        if (available && !provider.isAvailable()) {
            throw new IllegalStateException("The specified RangeReaderProvider is not available: " + providerId);
        }
        return provider;
    }
}
