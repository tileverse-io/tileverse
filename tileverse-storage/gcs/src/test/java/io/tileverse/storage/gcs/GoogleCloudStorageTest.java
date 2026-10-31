/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageOptions;
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.WriteOptions;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Drives a storage over a mocked client. A request with an ETag condition must require the generation and
 * metageneration of the blob holding that ETag: another write between the ETag check and the request then fails it.
 */
@ExtendWith(MockitoExtension.class)
class GoogleCloudStorageTest {

    private static final URI BASE_URI = URI.create("gs://bucket/");
    private static final String ETAG = "CAcQAw==";
    private static final long GENERATION = 7L;
    private static final long METAGENERATION = 3L;

    @Mock
    com.google.cloud.storage.Storage client;

    @Mock
    Blob blob;

    @BeforeEach
    void stubTheBlob() {
        when(client.getOptions())
                .thenReturn(StorageOptions.newBuilder()
                        .setProjectId("test-project")
                        .setCredentials(NoCredentials.getInstance())
                        .build());
        // The storage looks up its bucket when it opens: a missing bucket reads as one without a hierarchical namespace
        when(client.get(any(String.class), any(BucketGetOption[].class))).thenReturn(null);
        when(client.get(any(BlobId.class), any(BlobGetOption[].class))).thenReturn(blob);
        when(blob.getEtag()).thenReturn(ETAG);
        when(blob.getGeneration()).thenReturn(GENERATION);
        when(blob.getMetageneration()).thenReturn(METAGENERATION);
    }

    private GoogleCloudStorage openStorage() {
        SdkStorageLocation location = SdkStorageLocation.parse(BASE_URI);
        return new GoogleCloudStorage(BASE_URI, location, new BorrowedGcsHandle(client), Optional.empty());
    }

    @Test
    void etagPutRequiresGeneration() {
        when(client.create(any(), any(byte[].class), any(BlobTargetOption[].class)))
                .thenReturn(blob);
        try (GoogleCloudStorage storage = openStorage()) {
            storage.put("a.bin", new byte[] {1}, ifMatch());
        }

        ArgumentCaptor<BlobTargetOption[]> captor = ArgumentCaptor.forClass(BlobTargetOption[].class);
        verify(client).create(any(), any(byte[].class), captor.capture());
        assertThat(captor.getValue())
                .contains(
                        BlobTargetOption.generationMatch(GENERATION),
                        BlobTargetOption.metagenerationMatch(METAGENERATION));
    }

    @Test
    void etagFilePutRequiresGeneration(@TempDir Path tmp) throws IOException {
        Path source = Files.write(tmp.resolve("a.bin"), new byte[] {1});
        try (GoogleCloudStorage storage = openStorage()) {
            storage.put("a.bin", source, ifMatch());
        }

        ArgumentCaptor<BlobWriteOption[]> captor = ArgumentCaptor.forClass(BlobWriteOption[].class);
        verify(client).createFrom(any(), any(Path.class), captor.capture());
        assertThat(captor.getValue())
                .contains(
                        BlobWriteOption.generationMatch(GENERATION),
                        BlobWriteOption.metagenerationMatch(METAGENERATION));
    }

    @Test
    void etagStreamRequiresGeneration(@Mock WriteChannel channel) {
        when(client.writer(any(), any(BlobWriteOption[].class))).thenReturn(channel);
        try (GoogleCloudStorage storage = openStorage()) {
            // An unclosed stream sends nothing more: the conditions are set when the upload session opens
            storage.openOutputStream("a.bin", ifMatch());
        }

        ArgumentCaptor<BlobWriteOption[]> captor = ArgumentCaptor.forClass(BlobWriteOption[].class);
        verify(client).writer(any(), captor.capture());
        assertThat(captor.getValue())
                .contains(
                        BlobWriteOption.generationMatch(GENERATION),
                        BlobWriteOption.metagenerationMatch(METAGENERATION));
    }

    @Test
    void etagCopyRequiresGeneration(@Mock CopyWriter copyWriter) {
        when(client.copy(any(CopyRequest.class))).thenReturn(copyWriter);
        CopyOptions ifMatch = CopyOptions.builder().ifMatchSourceEtag(ETAG).build();
        try (GoogleCloudStorage storage = openStorage()) {
            storage.copy("a.bin", "b.bin", ifMatch);
        }

        ArgumentCaptor<CopyRequest> captor = ArgumentCaptor.forClass(CopyRequest.class);
        verify(client).copy(captor.capture());
        assertThat(captor.getValue().getSourceOptions())
                .contains(
                        BlobSourceOption.generationMatch(GENERATION),
                        BlobSourceOption.metagenerationMatch(METAGENERATION));
    }

    private static WriteOptions ifMatch() {
        return WriteOptions.builder().ifMatchEtag(ETAG).build();
    }
}
