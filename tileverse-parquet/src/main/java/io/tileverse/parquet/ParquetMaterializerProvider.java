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
package io.tileverse.parquet;

import java.util.Map;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * Factory for creating a {@link RecordMaterializer} that converts Parquet column data into
 * records of type {@code T}.
 *
 * @param <T> the record type produced by the materializer
 * @see GroupMaterializerProvider
 * @see AvroMaterializerProvider
 */
@FunctionalInterface
public interface ParquetMaterializerProvider<T> {

    /**
     * Creates a {@link RecordMaterializer} for the given schemas and file metadata.
     *
     * <p>The returned materializer is reused across all row groups in the file.
     *
     * @param fileSchema      the full Parquet file schema
     * @param requestedSchema the projected schema (subset of columns to materialize)
     * @param fileMetadata    the file-level key-value metadata
     * @return a materializer that converts column data into records of type {@code T}
     */
    RecordMaterializer<T> createMaterializer(
            MessageType fileSchema, MessageType requestedSchema, Map<String, String> fileMetadata);
}
