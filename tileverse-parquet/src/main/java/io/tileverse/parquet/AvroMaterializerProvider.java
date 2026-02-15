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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * A {@link ParquetMaterializerProvider} that produces Avro {@link GenericRecord} instances
 * using {@link AvroReadSupport}.
 *
 * <p>Uses {@link GenericData#get()} injected via the {@link AvroReadSupport#AvroReadSupport(GenericData)}
 * constructor to bypass a {@link PlainParquetConfiguration#getClass(String, Class, Class)} bug
 * that silently ignores the configured {@code AVRO_DATA_SUPPLIER}.
 */
class AvroMaterializerProvider implements ParquetMaterializerProvider<GenericRecord> {

    static final AvroMaterializerProvider INSTANCE = new AvroMaterializerProvider();

    private AvroMaterializerProvider() {}

    @Override
    public RecordMaterializer<GenericRecord> createMaterializer(
            MessageType fileSchema, MessageType requestedSchema, Map<String, String> fileMetadata) {

        AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>(GenericData.get());
        PlainParquetConfiguration config = new PlainParquetConfiguration();

        // Let AvroReadSupport resolve internal metadata (AVRO_COMPATIBILITY, etc.)
        ReadSupport.ReadContext initContext = readSupport.init(config, fileMetadata, fileSchema);

        // Override the schema with our requestedSchema (for projection) while
        // preserving the readSupportMetadata that init() resolved
        Map<String, String> readSupportMetadata = initContext.getReadSupportMetadata();
        ReadSupport.ReadContext readContext = new ReadSupport.ReadContext(requestedSchema, readSupportMetadata);

        return readSupport.prepareForRead(config, fileMetadata, fileSchema, readContext);
    }
}
