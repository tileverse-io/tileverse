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
package io.tileverse.parquet.reader;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class CoreRowGroupMeta {
    private final long rowCount;
    private final List<CoreColumnChunkMeta> columns;

    CoreRowGroupMeta(long rowCount, List<CoreColumnChunkMeta> columns) {
        this.rowCount = rowCount;
        this.columns = Collections.unmodifiableList(List.copyOf(Objects.requireNonNull(columns, "columns")));
    }

    long rowCount() {
        return rowCount;
    }

    List<CoreColumnChunkMeta> columns() {
        return columns;
    }
}
