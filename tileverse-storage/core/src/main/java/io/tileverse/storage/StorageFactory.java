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

import io.tileverse.storage.spi.CachingProviderHelper;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.util.Properties;
import lombok.NonNull;

/**
 * Top-level entrypoint for opening a {@link Storage}. Selects the appropriate {@link StorageProvider} via
 * {@link StorageProviderResolver}, which handles unambiguous matches, HTTP HEAD-probing for ambiguous
 * {@code http(s)://} URIs, and priority-based tie-breaking.
 *
 * <p>The {@link Properties}-accepting overloads delegate to {@link StorageConfig#fromProperties(Properties)} - this is
 * the bridge for tools (GeoTools datastores, Spring config binding, etc.) that pass provider configuration as a map.
 *
 * <p>For byte-range reads of a single object, open a Storage rooted at the object's container/parent and call
 * {@link Storage#openRangeReader(String)} with the relative key. The Storage's lifetime governs the SDK client's
 * lifetime; close the Storage when done. {@code RangeReader} obtained from a {@code Storage} releases per-blob state on
 * its own close, but the Storage itself remains open until the caller closes it.
 */
public final class StorageFactory {

    private StorageFactory() {}

    /**
     * Open a {@link Storage} rooted at {@code uri} using default configuration.
     *
     * @throws StorageException if the backend cannot be opened
     */
    public static Storage open(@NonNull URI uri) {
        return open(new StorageConfig().baseUri(uri));
    }

    /**
     * Open a {@link Storage} rooted at {@code uri}, configuring the backend from {@code props}. Properties keys live
     * under the {@code storage.*} namespace (legacy {@code io.tileverse.rangereader.*} keys are accepted with a
     * one-time WARN per distinct key). The URI argument wins over any {@code storage.uri} entry in the props.
     *
     * @throws StorageException if the backend cannot be opened
     */
    public static Storage open(@NonNull URI uri, @NonNull Properties props) {
        Properties merged = new Properties();
        merged.putAll(props);
        merged.setProperty("storage.uri", uri.toString());
        return open(StorageConfig.fromProperties(merged));
    }

    /**
     * Open a {@link Storage} from a fully-populated {@link StorageConfig}. The config's {@link StorageConfig#baseUri()}
     * drives provider selection.
     *
     * @throws StorageException if the backend cannot be opened
     */
    public static Storage open(@NonNull StorageConfig config) {
        StorageProvider provider = StorageProviderResolver.findBestProvider(config);
        StorageConfig resolved = withProviderDefaults(provider, config);
        Storage storage = provider.createStorage(resolved);
        return decorateForCaching(storage, resolved);
    }

    /**
     * Layer {@code config} over the provider's {@link StorageProvider#getDefaultConfig() default values} so that
     * {@link StorageParameter#defaultValue() declared defaults} take effect for any key the caller did not set
     * explicitly. This preserves the auto-caching behavior carried by the legacy single-URI factory path.
     */
    private static StorageConfig withProviderDefaults(
            @NonNull StorageProvider provider, @NonNull StorageConfig config) {
        StorageConfig merged = provider.getDefaultConfig();
        merged.baseUri(config.baseUri());
        config.providerId().ifPresent(merged::providerId);
        for (StorageParameter<?> param : provider.getParameters()) {
            config.getParameter(param.key()).ifPresent(v -> merged.setParameter(param.key(), v));
        }
        return merged;
    }

    /**
     * If the resolved config enables in-memory caching, wrap {@code storage} with a {@link CachingStorage} so each
     * {@link Storage#openRangeReader(String)} call returns a cached/block-aligned reader, matching the behavior of the
     * legacy provider-side auto-decoration.
     */
    private static Storage decorateForCaching(@NonNull Storage storage, @NonNull StorageConfig config) {
        return CachingProviderHelper.cachingDecoratorFor(config)
                .<Storage>map(d -> new CachingStorage(storage, d::apply))
                .orElse(storage);
    }

    /**
     * Open a {@link Storage} entirely from {@link Properties}. The URI must be present in the props as
     * {@code storage.uri} (or legacy {@code io.tileverse.rangereader.uri}). This is the entry point for GeoTools-style
     * datastore parameter maps.
     *
     * @throws StorageException if the backend cannot be opened
     */
    public static Storage open(@NonNull Properties props) {
        return open(StorageConfig.fromProperties(props));
    }

    /**
     * Look up the {@link StorageProvider} that would be selected for {@code config}, without opening a {@link Storage}.
     * Useful for testing provider-selection logic and for callers that want to inspect provider metadata before
     * opening.
     */
    public static StorageProvider findProvider(@NonNull StorageConfig config) {
        return StorageProviderResolver.findBestProvider(config);
    }
}
