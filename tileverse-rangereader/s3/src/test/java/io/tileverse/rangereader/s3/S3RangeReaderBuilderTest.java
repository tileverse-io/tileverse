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
package io.tileverse.rangereader.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

class S3RangeReaderBuilderTest {

    @Test
    void testNullValidationOnSetters() {
        assertThrows(NullPointerException.class, () -> S3RangeReader.builder().s3Client(null));
        assertThrows(NullPointerException.class, () -> S3RangeReader.builder().credentialsProvider(null));
        assertThrows(NullPointerException.class, () -> S3RangeReader.builder().region(null));
        assertThrows(NullPointerException.class, () -> S3RangeReader.builder().endpoint(null));
        assertThrows(NullPointerException.class, () -> S3RangeReader.builder().bucket(null));
        assertThrows(NullPointerException.class, () -> S3RangeReader.builder().key(null));
        assertThrows(NullPointerException.class, () -> S3RangeReader.builder().uri((URI) null));
    }

    @Test
    void testUriStringParsesVirtualHostedStyleUrl() throws Exception {
        S3RangeReader.Builder builder =
                S3RangeReader.builder().uri("https://bucket.s3.us-west-2.amazonaws.com/path/file.pmtiles");

        S3Reference location = getField(builder, "s3Location", S3Reference.class);
        assertEquals("bucket", location.bucket());
        assertEquals("path/file.pmtiles", location.key());
        assertEquals("us-west-2", location.region());
        assertFalse(location.requiresPathStyle());
    }

    @Test
    void testForcePathStyleFalseCanOverrideAutomaticFlag() throws Exception {
        S3RangeReader.Builder builder = S3RangeReader.builder()
                .uri(URI.create("http://localhost:9000/my-bucket/file.pmtiles"))
                .forcePathStyle(false);

        assertFalse(getField(builder, "forcePathStyle", Boolean.class));
        S3Reference location = getField(builder, "s3Location", S3Reference.class);
        assertTrue(location.requiresPathStyle());
    }

    @Test
    void testResolveRegionReturnsEmptyWhenNothingConfigured() throws Exception {
        Optional<Region> region =
                invokeResolveRegion(S3RangeReader.builder().bucket("bucket").key("file"));
        assertTrue(region.isEmpty());
    }

    @Test
    void testDefaultCredentialsProfileSetterStoresValue() throws Exception {
        S3RangeReader.Builder builder = S3RangeReader.builder().defaultCredentialsProfile("named-profile");
        assertEquals("named-profile", getField(builder, "defaultCredentialsProfile", String.class));
        assertNotNull(invokeResolveCredentialsProvider(builder));
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Object target, String name, Class<T> type) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(target);
    }

    @SuppressWarnings("unchecked")
    private static Optional<Region> invokeResolveRegion(S3RangeReader.Builder builder) throws Exception {
        Method method = builder.getClass().getDeclaredMethod("resolveRegion");
        method.setAccessible(true);
        return (Optional<Region>) method.invoke(builder);
    }

    private static Object invokeResolveCredentialsProvider(S3RangeReader.Builder builder) throws Exception {
        Method method = builder.getClass().getDeclaredMethod("resolveCredentialsProvider");
        method.setAccessible(true);
        return method.invoke(builder);
    }
}
