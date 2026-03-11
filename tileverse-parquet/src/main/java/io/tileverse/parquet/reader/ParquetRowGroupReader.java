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

import java.io.Closeable;
import java.io.IOException;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.MessageType;

/**
 * Reads row groups from a Parquet file, returning page data for column assembly.
 *
 * <p>Implementations may apply filter pushdown to skip row groups or pages that cannot
 * match the configured predicate.
 */
public interface ParquetRowGroupReader extends Closeable {
    /**
     * Sets the projected schema, restricting which columns are read.
     *
     * @param requestedSchema the schema containing only the requested columns
     */
    void setRequestedSchema(MessageType requestedSchema);

    /**
     * Reads the next row group without filter pushdown.
     *
     * @return the page store for the next row group, or {@code null} if no more row groups
     * @throws IOException if an I/O error occurs
     */
    PageReadStore readNextRowGroup() throws IOException;

    /**
     * Reads the next row group, skipping groups eliminated by filter pushdown.
     *
     * @return the page store for the next matching row group, or {@code null} if no more match
     * @throws IOException if an I/O error occurs
     */
    PageReadStore readNextFilteredRowGroup() throws IOException;
}
