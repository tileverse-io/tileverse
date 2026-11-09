/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.cache.CacheBudgets;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CacheStats;
import io.tileverse.cache.CaffeineCacheManager;
import io.tileverse.tiling.pyramid.TileIndex;
import io.tileverse.vectortile.model.VectorTile;
import io.tileverse.vectortile.store.VectorTileCache.VectorTileLoader;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class VectorTileCacheTest {

    private static final TileIndex TILE = TileIndex.zxy(0, 0, 0);

    private final AtomicLong nanos = new AtomicLong();

    private final CacheManager manager = newManager();

    @Test
    void loadsOncePerTile() throws IOException {
        CountingLoader loader = new CountingLoader();
        VectorTileCache cache = newCache("a@1", loader);

        Optional<VectorTile> first = cache.getVectorTile(TILE);
        Optional<VectorTile> second = cache.getVectorTile(TILE);

        assertThat(first).isEmpty();
        assertThat(second).isEmpty();
        assertThat(loader.calls()).isEqualTo(1);
    }

    @Test
    void partitionLivesInTheDataPool() throws IOException {
        VectorTileCache cache = newCache("a@1", new CountingLoader());

        cache.getVectorTile(TILE);

        assertThat(manager.getCacheNames()).contains("tileverse-cache-data", "tileverse-vectortile-cache");
    }

    @Test
    void sourcesDoNotShareTiles() throws IOException {
        CountingLoader firstLoader = new CountingLoader();
        CountingLoader secondLoader = new CountingLoader();
        VectorTileCache first = newCache("a@1", firstLoader);
        VectorTileCache second = newCache("a@2", secondLoader);

        first.getVectorTile(TILE);
        second.getVectorTile(TILE);

        assertThat(firstLoader.calls()).isEqualTo(1);
        assertThat(secondLoader.calls()).isEqualTo(1);
    }

    @Test
    void expiresTenSecondsAfterAccess() throws IOException {
        CountingLoader loader = new CountingLoader();
        VectorTileCache cache = newCache("a@1", loader);
        cache.getVectorTile(TILE);

        advance(Duration.ofSeconds(9));
        cache.getVectorTile(TILE);
        assertThat(loader.calls()).isEqualTo(1);

        advance(Duration.ofSeconds(11));
        cache.getVectorTile(TILE);
        assertThat(loader.calls()).isEqualTo(2);
    }

    @Test
    void setCacheManagerDropsEntries() throws IOException {
        VectorTileCache cache = newCache("a@1", new CountingLoader());
        cache.getVectorTile(TILE);
        assertThat(firstManagerStats().entryCount()).isEqualTo(1);

        cache.setCacheManager(newManager());

        assertThat(firstManagerStats().entryCount()).isZero();
    }

    @Test
    void loaderFailureReachesCaller() {
        IOException failure = new IOException("unreadable tile");
        VectorTileCache cache = newCache("a@1", tileIndex -> {
            throw failure;
        });

        assertThatThrownBy(() -> cache.getVectorTile(TILE)).isSameAs(failure);
    }

    private CacheManager newManager() {
        CacheBudgets budgets = new CacheBudgets(1 << 20, 0);
        return new CaffeineCacheManager(budgets, nanos::get);
    }

    private VectorTileCache newCache(String contentIdentifier, VectorTileLoader loader) {
        VectorTileCache cache = new VectorTileCache(contentIdentifier, loader);
        cache.setCacheManager(manager);
        return cache;
    }

    private void advance(Duration duration) {
        nanos.addAndGet(duration.toNanos());
    }

    private CacheStats firstManagerStats() {
        return manager.stats().get("tileverse-vectortile-cache");
    }

    /** Answers that no tile exists, counting its calls. */
    private static final class CountingLoader implements VectorTileLoader {

        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public Optional<VectorTile> load(TileIndex tileIndex) {
            calls.incrementAndGet();
            return Optional.empty();
        }

        int calls() {
            return calls.get();
        }
    }
}
