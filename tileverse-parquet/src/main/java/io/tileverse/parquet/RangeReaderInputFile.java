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
package io.tileverse.parquet;

import io.tileverse.rangereader.RangeReader;
import java.io.IOException;
import java.util.Objects;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * An {@link InputFile} implementation backed by a {@link RangeReader}.
 * <p>
 * This adapter enables Apache Parquet readers ({@code ParquetFileReader}, {@code ParquetReader})
 * to read from any source supported by the tileverse-rangereader library: local files, HTTP
 * servers, and cloud storage (S3, Azure, GCS).
 * <p>
 * <strong>Lifecycle:</strong> This class does not take ownership of the underlying
 * {@code RangeReader}. The caller is responsible for closing the {@code RangeReader}
 * when it is no longer needed. Multiple streams can be created from the same
 * {@code RangeReaderInputFile} concurrently, as {@code RangeReader} is thread-safe.
 *
 * <p><strong>Usage example:</strong></p>
 * <pre>{@code
 * try (RangeReader reader = HttpRangeReader.of("https://example.com/data.parquet")) {
 *     InputFile inputFile = new RangeReaderInputFile(reader);
 *     try (ParquetFileReader parquetReader = ParquetFileReader.open(inputFile)) {
 *         ParquetMetadata metadata = parquetReader.getFooter();
 *         // read row groups...
 *     }
 * }
 * }</pre>
 */
public class RangeReaderInputFile implements InputFile {

    private final RangeReader rangeReader;

    /**
     * Creates a new {@code RangeReaderInputFile} backed by the given {@code RangeReader}.
     *
     * @param rangeReader the range reader to use for file access
     * @throws NullPointerException if {@code rangeReader} is null
     */
    public RangeReaderInputFile(RangeReader rangeReader) {
        this.rangeReader = Objects.requireNonNull(rangeReader, "rangeReader");
    }

    @Override
    public long getLength() throws IOException {
        return rangeReader
                .size()
                .orElseThrow(() -> new IOException("Cannot determine size of " + rangeReader.getSourceIdentifier()));
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new RangeReaderSeekableInputStream(rangeReader);
    }

    @Override
    public String toString() {
        return "RangeReaderInputFile[" + rangeReader.getSourceIdentifier() + "]";
    }
}
