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
package io.tileverse.parquet.reader;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.parquet.schema.MessageType;

public final class CoreParquetFooter {
    private final MessageType schema;
    private final Map<String, String> keyValueMetadata;
    private final long recordCount;
    private final List<CoreRowGroupMeta> rowGroups;

    CoreParquetFooter(
            MessageType schema,
            Map<String, String> keyValueMetadata,
            long recordCount,
            List<CoreRowGroupMeta> rowGroups) {
        this.schema = Objects.requireNonNull(schema, "schema");
        this.keyValueMetadata = Collections.unmodifiableMap(new LinkedHashMap<>(keyValueMetadata));
        this.recordCount = recordCount;
        this.rowGroups = List.copyOf(Objects.requireNonNull(rowGroups, "rowGroups"));
    }

    public MessageType schema() {
        return schema;
    }

    public Map<String, String> keyValueMetadata() {
        return keyValueMetadata;
    }

    public long recordCount() {
        return recordCount;
    }

    public List<CoreRowGroupMeta> rowGroups() {
        return rowGroups;
    }
}
