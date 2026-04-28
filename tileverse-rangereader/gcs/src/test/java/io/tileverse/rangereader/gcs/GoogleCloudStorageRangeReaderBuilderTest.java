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
package io.tileverse.rangereader.gcs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.storage.StorageException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import org.junit.jupiter.api.Test;

class GoogleCloudStorageRangeReaderBuilderTest {

    @Test
    void testNullValidationOnSetters() {
        assertThrows(
                NullPointerException.class,
                () -> GoogleCloudStorageRangeReader.builder().storage(null));
        assertThrows(
                NullPointerException.class,
                () -> GoogleCloudStorageRangeReader.builder().projectId(null));
        assertThrows(
                NullPointerException.class,
                () -> GoogleCloudStorageRangeReader.builder().bucket(null));
        assertThrows(
                NullPointerException.class,
                () -> GoogleCloudStorageRangeReader.builder().objectName(null));
        assertThrows(
                NullPointerException.class,
                () -> GoogleCloudStorageRangeReader.builder().quotaProjectId(null));
        assertThrows(
                NullPointerException.class,
                () -> GoogleCloudStorageRangeReader.builder().uri(null));
    }

    @Test
    void testGsUriParsing() throws Exception {
        GoogleCloudStorageRangeReader.Builder builder =
                GoogleCloudStorageRangeReader.builder().uri(URI.create("gs://bucket/path/file.pmtiles"));

        assertEquals("bucket", getField(builder, "bucket"));
        assertEquals("path/file.pmtiles", getField(builder, "objectName"));
    }

    @Test
    void testCustomPublicHttpUriSetsHost() throws Exception {
        GoogleCloudStorageRangeReader.Builder builder = GoogleCloudStorageRangeReader.builder()
                .uri(URI.create("https://storage.my-company.test/bucket/path/file.pmtiles"));

        assertEquals("bucket", getField(builder, "bucket"));
        assertEquals("path/file.pmtiles", getField(builder, "objectName"));
        assertEquals("https://storage.my-company.test", getField(builder, "host"));
    }

    @Test
    void testStandardPublicHttpUriDoesNotSetHostOverride() throws Exception {
        GoogleCloudStorageRangeReader.Builder builder = GoogleCloudStorageRangeReader.builder()
                .uri(URI.create("https://storage.googleapis.com/bucket/file.pmtiles"));

        assertEquals("bucket", getField(builder, "bucket"));
        assertEquals("file.pmtiles", getField(builder, "objectName"));
        assertEquals(null, getField(builder, "host"));
    }

    @Test
    void testApiUriDecodesObjectAndSetsHost() throws Exception {
        GoogleCloudStorageRangeReader.Builder builder = GoogleCloudStorageRangeReader.builder()
                .uri(URI.create("http://localhost:4443/storage/v1/b/test-bucket/o/path%2Fto%2Ffile.pmtiles"));

        assertEquals("test-bucket", getField(builder, "bucket"));
        assertEquals("path/to/file.pmtiles", getField(builder, "objectName"));
        assertEquals("http://localhost:4443", getField(builder, "host"));
    }

    @Test
    void testBuilderStoresProjectQuotaAndDefaultCredentialsFlags() throws Exception {
        GoogleCloudStorageRangeReader.Builder builder = GoogleCloudStorageRangeReader.builder()
                .projectId("project")
                .quotaProjectId("quota-project")
                .defaultCredentialsChain(true)
                .bucket("bucket")
                .objectName("file.pmtiles");

        assertEquals("project", getField(builder, "projectId"));
        assertEquals("quota-project", getField(builder, "quotaProjectId"));
        assertTrue((Boolean) getField(builder, "defaultCredentialsChain"));
    }

    @Test
    void testInvalidUrisAndMissingRequiredValues() {
        assertThrows(
                IllegalArgumentException.class,
                () -> GoogleCloudStorageRangeReader.builder().uri(URI.create("ftp://bucket/file.pmtiles")));
        assertThrows(
                IllegalArgumentException.class,
                () -> GoogleCloudStorageRangeReader.builder().uri(URI.create("gs:///file.pmtiles")));
        assertThrows(
                IllegalArgumentException.class,
                () -> GoogleCloudStorageRangeReader.builder().uri(URI.create("https://storage.googleapis.com/bucket")));
        assertThrows(
                IllegalArgumentException.class,
                () -> GoogleCloudStorageRangeReader.builder()
                        .uri(URI.create("https://storage.googleapis.com//file.pmtiles")));

        IllegalStateException ex = assertThrows(
                IllegalStateException.class,
                () -> GoogleCloudStorageRangeReader.builder().bucket("bucket").build());
        assertNotNull(ex.getMessage());
    }

    @Test
    void testBuildWithoutStorageFallsBackToAnonymousBeforeNetworkAccess() {
        Throwable ex = assertThrows(
                Throwable.class,
                () -> GoogleCloudStorageRangeReader.builder()
                        .bucket("bucket")
                        .objectName("missing-object")
                        .build());
        assertTrue(ex instanceof IOException || ex instanceof StorageException);
        assertFalse(ex.getMessage().isBlank());
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
