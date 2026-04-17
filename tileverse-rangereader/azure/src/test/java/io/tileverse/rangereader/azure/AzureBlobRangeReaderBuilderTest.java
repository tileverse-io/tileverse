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
package io.tileverse.rangereader.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

class AzureBlobRangeReaderBuilderTest {

    @Test
    void testNullValidationOnSetters() {
        assertThrows(
                NullPointerException.class, () -> AzureBlobRangeReader.builder().tokenCredential(null));
        assertThrows(
                NullPointerException.class, () -> AzureBlobRangeReader.builder().accountName(null));
        assertThrows(
                NullPointerException.class, () -> AzureBlobRangeReader.builder().accountKey(null));
        assertThrows(
                NullPointerException.class, () -> AzureBlobRangeReader.builder().connectionString(null));
        assertThrows(
                NullPointerException.class, () -> AzureBlobRangeReader.builder().containerName(null));
        assertThrows(
                NullPointerException.class, () -> AzureBlobRangeReader.builder().blobName(null));
        assertThrows(
                NullPointerException.class, () -> AzureBlobRangeReader.builder().sasToken(null));
        assertThrows(
                NullPointerException.class, () -> AzureBlobRangeReader.builder().endpoint(null));
    }

    @Test
    void testAccountCredentialsAndEndpointSetFields() throws Exception {
        AzureBlobRangeReader.Builder builder = AzureBlobRangeReader.builder()
                .accountCredentials("devstoreaccount1", "secret")
                .endpoint(URI.create("http://127.0.0.1:10000/devstoreaccount1/container/blob.pmtiles"));

        assertEquals("devstoreaccount1", getField(builder, "accountName"));
        assertEquals("secret", getField(builder, "accountKey"));
        assertEquals(
                URI.create("http://127.0.0.1:10000/devstoreaccount1/container/blob.pmtiles"),
                getField(builder, "endpoint"));
    }

    @Test
    void testConnectionStringAndBlobFields() throws Exception {
        AzureBlobRangeReader.Builder builder = AzureBlobRangeReader.builder()
                .connectionString(
                        "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=secret;EndpointSuffix=core.windows.net")
                .containerName("container")
                .blobName("path/blob.pmtiles")
                .sasToken("sig=123");

        assertEquals("container", getField(builder, "containerName"));
        assertEquals("path/blob.pmtiles", getField(builder, "blobName"));
        assertEquals("sig=123", getField(builder, "sasToken"));
        assertNotNull(getField(builder, "connectionString"));
    }

    @Test
    void testEndpointSchemeValidationAndMissingRequirements() {
        assertThrows(IllegalArgumentException.class, () -> AzureBlobRangeReader.builder()
                .endpoint(URI.create("gs://bucket/file")));

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> AzureBlobRangeReader.builder()
                .connectionString("UseDevelopmentStorage=true")
                .build());
        assertTrue(ex.getMessage().contains("Container name"));
    }

    @Test
    void testBuildWithIncompleteConnectionStringFailsFast() {
        // Point to localhost:1 (instant connection refused, no DNS or TCP timeout)
        // and disable retries so the test completes in under a second.
        IOException ex = assertThrows(IOException.class, () -> AzureBlobRangeReader.builder()
                .connectionString("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
                        + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                        + "BlobEndpoint=http://127.0.0.1:1/devstoreaccount1")
                .containerName("container")
                .blobName("blob.pmtiles")
                .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 1, (Integer) null, null, null, null))
                .build());
        assertNotNull(ex.getMessage());
    }

    @Test
    @ResourceLock(Resources.SYSTEM_PROPERTIES)
    void testProviderAvailabilityToggle() {
        AzureBlobRangeReaderProvider provider = new AzureBlobRangeReaderProvider();
        assertTrue(provider.isAvailable());

        System.setProperty(AzureBlobRangeReaderProvider.ENABLED_KEY, "false");
        try {
            assertEquals(false, provider.isAvailable());
            assertTrue(RangeReaderProvider.findProvider(AzureBlobRangeReaderProvider.ID)
                    .isPresent());
        } finally {
            System.clearProperty(AzureBlobRangeReaderProvider.ENABLED_KEY);
        }
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
