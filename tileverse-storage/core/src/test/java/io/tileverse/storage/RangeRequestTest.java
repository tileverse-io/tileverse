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

import io.tileverse.io.ByteRange;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class RangeRequestTest {

    @Test
    void constructorValidates() {
        ByteBuffer target = ByteBuffer.allocate(10);
        assertThatThrownBy(() -> new RangeRequest(null, target)).isInstanceOf(NullPointerException.class);

        ByteRange range = ByteRange.of(0, 5);
        assertThatThrownBy(() -> new RangeRequest(range, null)).isInstanceOf(NullPointerException.class);

        ByteBuffer readOnly = target.asReadOnlyBuffer();
        assertThatThrownBy(() -> new RangeRequest(range, readOnly)).isInstanceOf(ReadOnlyBufferException.class);

        ByteRange tooLarge = ByteRange.of(0, 11);
        assertThatThrownBy(() -> new RangeRequest(tooLarge, target))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remaining");
    }

    @Test
    void factoriesBuildEquivalentRequests() {
        ByteBuffer target = ByteBuffer.allocate(10);
        RangeRequest fromRange = RangeRequest.of(ByteRange.of(3, 4), target);
        RangeRequest fromOffsets = RangeRequest.of(3, 4, target);
        assertThat(fromOffsets.range()).isEqualTo(fromRange.range());
        assertThat(fromOffsets.target()).isSameAs(target);
    }

    @Test
    void remainingCapacityCountsFromCurrentPosition() {
        ByteBuffer target = ByteBuffer.allocate(10);
        target.position(6);
        assertThatThrownBy(() -> RangeRequest.of(0, 5, target)).isInstanceOf(IllegalArgumentException.class);
        RangeRequest ok = RangeRequest.of(0, 4, target);
        assertThat(ok.target().remaining()).isEqualTo(4);
    }

    @Test
    void validateRejectsMalformedBatches() {
        assertThatThrownBy(() -> RangeRequest.validate(null)).isInstanceOf(NullPointerException.class);

        List<RangeRequest> withNull = new ArrayList<>();
        withNull.add(RangeRequest.of(0, 4, ByteBuffer.allocate(4)));
        withNull.add(null);
        assertThatThrownBy(() -> RangeRequest.validate(withNull))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("index 1");

        // capacity that shrank after construction (position moved) is caught at validate time
        ByteBuffer moved = ByteBuffer.allocate(10);
        RangeRequest request = RangeRequest.of(0, 8, moved);
        moved.position(5);
        List<RangeRequest> shrunkBatch = List.of(request);
        assertThatThrownBy(() -> RangeRequest.validate(shrunkBatch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("index 0");

        RangeRequest.validate(List.of()); // empty list is legal
    }
}
