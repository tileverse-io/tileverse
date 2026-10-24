/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
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
    void testCustomEndpointRendersWithoutTrailingSlash() {
        S3Reference reference = new S3Reference(URI.create("http://localhost:9000/"), "bucket", "file.pmtiles", null);

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
