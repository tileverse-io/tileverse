/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.cache;

import java.util.Objects;

class DefaultCacheManagerHolder {

    private static CacheManager instance = new CaffeineCacheManager();

    private DefaultCacheManagerHolder() {
        // private constructor, utility class
    }

    static CacheManager get() {
        return instance;
    }

    static void set(CacheManager newDefault) {
        Objects.requireNonNull(newDefault);
        CacheManager oldInstance = instance;
        instance = newDefault;
        oldInstance.invalidateAll();
    }
}
