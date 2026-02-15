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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

/**
 * A {@link ParquetMaterializerProvider} that produces {@link Group} records using
 * {@link GroupRecordConverter}.
 */
class GroupMaterializerProvider implements ParquetMaterializerProvider<Group> {

    static final GroupMaterializerProvider INSTANCE = new GroupMaterializerProvider();

    private GroupMaterializerProvider() {}

    @Override
    public RecordMaterializer<Group> createMaterializer(
            MessageType fileSchema, MessageType requestedSchema, Map<String, String> fileMetadata) {

        GroupRecordConverter converter = new GroupRecordConverter(requestedSchema);
        return new RecordMaterializer<>() {
            @Override
            public Group getCurrentRecord() {
                return converter.getCurrentRecord();
            }

            @Override
            public GroupConverter getRootConverter() {
                return converter.getRootConverter();
            }
        };
    }
}
