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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tileverse.storage.file.FileStorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.util.List;
import org.junit.jupiter.api.Test;

class StorageProviderResolverTest {

    private static final String TIE_SCHEME = "tie";

    private final StorageConfig config = new StorageConfig(TIE_SCHEME + "://host/path");

    @Test
    void theLowestOrderWinsAmongProvidersClaimingOneScheme() {
        StorageProvider preferred = new SchemeClaimingProvider("preferred", -10);
        StorageProvider other = new SchemeClaimingProvider("other", 0);

        StorageProvider selected = StorageProviderResolver.selectByUri(config, List.of(other, preferred));

        assertThat(selected).isSameAs(preferred);
    }

    @Test
    void equalOrdersRaiseTheAmbiguityError() {
        List<StorageProvider> tied =
                List.of(new SchemeClaimingProvider("first", 0), new SchemeClaimingProvider("second", 0));

        assertThatThrownBy(() -> StorageProviderResolver.selectByUri(config, tied))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("URI ambiguity detected")
                .hasMessageContaining("first, second");
    }

    @Test
    void aSingleClaimIsSelectedWhateverItsOrder() {
        StorageProvider only = new SchemeClaimingProvider("only", 100);

        StorageProvider selected = StorageProviderResolver.selectByUri(config, List.of(only));

        assertThat(selected).isSameAs(only);
    }

    @Test
    void noClaimIsAnError() {
        List<StorageProvider> none = List.of();

        assertThatThrownBy(() -> StorageProviderResolver.selectByUri(config, none))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No suitable provider found for URI: tie://host/path");
    }

    @Test
    void aNamedProviderIsReturnedWithoutAskingWhetherItClaimsTheUri() {
        StorageConfig namingFile = new StorageConfig(TIE_SCHEME + "://host/path").providerId("file");

        StorageProvider selected = StorageProviderResolver.findBestProvider(namingFile);

        assertThat(selected).isInstanceOf(FileStorageProvider.class);
    }

    @Test
    void anUnknownNamedProviderIsAnError() {
        StorageConfig namingUnknown = new StorageConfig("file:///data/").providerId("no-such-provider");

        assertThatThrownBy(() -> StorageProviderResolver.findBestProvider(namingUnknown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no-such-provider");
    }

    /** Claims the {@code tie} scheme; stands for a third-party provider competing for one scheme. */
    private record SchemeClaimingProvider(String id, int order) implements StorageProvider {

        @Override
        public String getId() {
            return id;
        }

        @Override
        public String getDescription() {
            return id;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public List<StorageParameter<?>> getParameters() {
            return List.of();
        }

        @Override
        public boolean canProcess(StorageConfig config) {
            return matches(config, TIE_SCHEME);
        }

        @Override
        public int getOrder() {
            return order;
        }

        @Override
        public Storage createStorage(StorageConfig config) {
            throw new UnsupportedOperationException();
        }
    }
}
