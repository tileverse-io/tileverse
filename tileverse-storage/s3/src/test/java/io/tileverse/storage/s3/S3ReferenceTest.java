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
package io.tileverse.storage.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import org.junit.jupiter.api.Test;

class S3ReferenceTest {

    @Test
    void testDefaultAwsReference() {
        S3Reference reference = new S3Reference(null, "bucket", "folder/file.pmtiles", null);

        assertTrue(reference.isDefaultAwsEndpoint());
        assertFalse(reference.requiresPathStyle());
        assertTrue(reference.endpointOverride().isEmpty());
        assertEquals("s3://bucket/folder/file.pmtiles", reference.toString());
    }

    @Test
    void testCustomEndpointReference() {
        URI endpoint = URI.create("http://localhost:9000");
        S3Reference reference = new S3Reference(endpoint, "bucket", "file.pmtiles", "us-east-1");

        assertFalse(reference.isDefaultAwsEndpoint());
        assertTrue(reference.requiresPathStyle());
        assertEquals(endpoint, reference.endpointOverride().orElseThrow());
        assertEquals("http://localhost:9000/bucket/file.pmtiles", reference.toString());
    }

    @Test
    void testWithersKeepOtherFields() {
        S3Reference base = new S3Reference(null, "bucket", "file.pmtiles", "us-east-1");

        S3Reference updated = base.withEndpoint(URI.create("https://example.com"))
                .withBucket("other")
                .withKey("nested/file.bin")
                .withRegion("eu-west-1");

        assertEquals(URI.create("https://example.com"), updated.endpoint());
        assertEquals("other", updated.bucket());
        assertEquals("nested/file.bin", updated.key());
        assertEquals("eu-west-1", updated.region());
    }
}
