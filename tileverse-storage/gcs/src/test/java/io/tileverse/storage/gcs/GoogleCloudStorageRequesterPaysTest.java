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
package io.tileverse.storage.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.Storage.SignUrlOption;
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.WriteOptions;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Verifies {@link GoogleCloudStorageProvider#GCS_USER_PROJECT}: every call site attaches a {@code userProject} option
 * (or, for presigned URLs, a query parameter) when the parameter is set, and attaches nothing when it is unset.
 *
 * <p>Implementation detail: the Google Cloud Storage Java SDK has no per-client {@code userProject} setting; the option
 * must be threaded into each per-call options array. This test mocks {@link com.google.cloud.storage.Storage} and
 * captures those arrays directly.
 *
 * <p>Equality of {@code Blob*Option}/{@code Bucket*Option} relies on the underlying {@code RpcOptVal} record-style
 * {@code equals/hashCode}, so {@code BlobGetOption.userProject("p")} is comparable to itself across instances.
 */
@ExtendWith(MockitoExtension.class)
class GoogleCloudStorageRequesterPaysTest {

    private static final String BUCKET = "rp-bucket";
    private static final URI BASE_URI = URI.create("gs://" + BUCKET + "/");
    private static final String USER_PROJECT = "billing-project";

    @Mock
    com.google.cloud.storage.Storage client;

    @Mock
    Blob blob;

    @Mock
    Bucket bucket;

    @Mock
    Page<Blob> page;

    @Mock
    WriteChannel writeChannel;

    @Mock
    CopyWriter copyWriter;

    @BeforeEach
    void stubGenericResponses() throws Exception {
        // Lenient stubs — most tests exercise a subset of these.
        lenient()
                .when(client.get(any(BlobId.class), any(BlobGetOption[].class)))
                .thenReturn(blob);
        lenient()
                .when(client.get(any(String.class), any(BucketGetOption[].class)))
                .thenReturn(bucket);
        lenient()
                .when(client.list(any(String.class), any(BlobListOption[].class)))
                .thenReturn(page);
        lenient().when(page.iterateAll()).thenReturn(List.of());
        lenient().when(blob.getSize()).thenReturn(0L);
        lenient().when(blob.getEtag()).thenReturn("etag");
        lenient().when(blob.getGeneration()).thenReturn(1L);
        lenient().when(blob.exists()).thenReturn(true);
        lenient().when(client.copy(any(CopyRequest.class))).thenReturn(copyWriter);
        lenient().when(copyWriter.getResult()).thenReturn(blob);
        lenient().when(client.writer(any(), any(BlobWriteOption[].class))).thenReturn(writeChannel);
        lenient()
                .when(client.signUrl(any(), any(Long.class), any(), any(SignUrlOption[].class)))
                .thenReturn(new URL("https://example.com/signed"));
    }

    private GoogleCloudStorage openStorage(Optional<String> userProject) {
        SdkStorageLocation location = SdkStorageLocation.parse(BASE_URI);
        return new GoogleCloudStorage(BASE_URI, location, new BorrowedGcsHandle(client), userProject);
    }

    @Test
    void statSendsUserProject() {
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            storage.stat("a.bin");
        }
        ArgumentCaptor<BlobGetOption[]> captor = ArgumentCaptor.forClass(BlobGetOption[].class);
        verify(client).get(any(BlobId.class), captor.capture());
        // The constructor's detectHns also takes BucketGetOption — assert that too.
        assertThat(captor.getAllValues())
                .anySatisfy(opts -> assertThat(opts).contains(BlobGetOption.userProject(USER_PROJECT)));
    }

    @Test
    void listSendsUserProject() {
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            storage.list("**", io.tileverse.storage.ListOptions.defaults()).toList();
        }
        ArgumentCaptor<BlobListOption[]> captor = ArgumentCaptor.forClass(BlobListOption[].class);
        verify(client).list(any(String.class), captor.capture());
        assertThat(captor.getValue()).contains(BlobListOption.userProject(USER_PROJECT));
    }

    @Test
    void putBytesSendsUserProject() {
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            when(client.create(any(), any(byte[].class), any(BlobTargetOption[].class)))
                    .thenReturn(blob);
            storage.put("a.bin", new byte[] {1, 2, 3}, WriteOptions.defaults());
        }
        ArgumentCaptor<BlobTargetOption[]> captor = ArgumentCaptor.forClass(BlobTargetOption[].class);
        verify(client).create(any(), any(byte[].class), captor.capture());
        assertThat(captor.getValue()).contains(BlobTargetOption.userProject(USER_PROJECT));
    }

    @Test
    void putPathSendsUserProject(@TempDir Path tmp) throws Exception {
        Path src = tmp.resolve("src.bin");
        Files.write(src, new byte[] {1});
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            storage.put("a.bin", src, WriteOptions.defaults());
        }
        ArgumentCaptor<BlobWriteOption[]> captor = ArgumentCaptor.forClass(BlobWriteOption[].class);
        verify(client).createFrom(any(), any(Path.class), captor.capture());
        assertThat(captor.getValue()).contains(BlobWriteOption.userProject(USER_PROJECT));
    }

    @Test
    void openOutputStreamSendsUserProject() throws Exception {
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            // Don't actually close the stream — that would trigger our put() pipeline.
            storage.openOutputStream("a.bin", WriteOptions.defaults());
        }
        ArgumentCaptor<BlobWriteOption[]> captor = ArgumentCaptor.forClass(BlobWriteOption[].class);
        verify(client).writer(any(), captor.capture());
        assertThat(captor.getValue()).contains(BlobWriteOption.userProject(USER_PROJECT));
    }

    @Test
    void deleteSendsUserProject() {
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            storage.delete("a.bin");
        }
        ArgumentCaptor<BlobSourceOption[]> captor = ArgumentCaptor.forClass(BlobSourceOption[].class);
        verify(client).delete(any(BlobId.class), captor.capture());
        assertThat(captor.getValue()).contains(BlobSourceOption.userProject(USER_PROJECT));
    }

    @Test
    void deleteAllUsesPerIdLoopWhenUserProjectSet() {
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            storage.deleteAll(List.of("a.bin", "b.bin"));
        }
        // Each id should be deleted individually with userProject; the bulk overload must NOT be used.
        verify(client, never()).delete(any(Iterable.class));
        ArgumentCaptor<BlobSourceOption[]> captor = ArgumentCaptor.forClass(BlobSourceOption[].class);
        verify(client, org.mockito.Mockito.times(2)).delete(any(BlobId.class), captor.capture());
        assertThat(captor.getAllValues())
                .allSatisfy(opts -> assertThat(opts).contains(BlobSourceOption.userProject(USER_PROJECT)));
    }

    @Test
    void copyAttachesUserProjectToBothEnds() {
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            storage.copy("a.bin", "b.bin", CopyOptions.defaults());
        }
        ArgumentCaptor<CopyRequest> captor = ArgumentCaptor.forClass(CopyRequest.class);
        verify(client).copy(captor.capture());
        CopyRequest req = captor.getValue();
        assertThat(req.getSourceOptions()).contains(BlobSourceOption.userProject(USER_PROJECT));
        assertThat(req.getTargetOptions()).contains(BlobTargetOption.userProject(USER_PROJECT));
    }

    @Test
    void presignGetEmbedsUserProjectAsQueryParam() throws Exception {
        try (GoogleCloudStorage storage = openStorage(Optional.of(USER_PROJECT))) {
            storage.presignGet("a.bin", Duration.ofMinutes(5));
        }
        ArgumentCaptor<SignUrlOption[]> captor = ArgumentCaptor.forClass(SignUrlOption[].class);
        verify(client).signUrl(any(), any(Long.class), any(), captor.capture());
        // SignUrlOption has no equals(); compare by reflecting on its package-private (option, value) fields.
        java.lang.reflect.Field optionField = SignUrlOption.class.getDeclaredField("option");
        java.lang.reflect.Field valueField = SignUrlOption.class.getDeclaredField("value");
        optionField.setAccessible(true);
        valueField.setAccessible(true);
        boolean hasUserProjectQueryParam = false;
        for (SignUrlOption opt : captor.getValue()) {
            String optName = optionField.get(opt).toString();
            Object value = valueField.get(opt);
            if ("QUERY_PARAMS".equals(optName)
                    && value instanceof java.util.Map<?, ?> map
                    && USER_PROJECT.equals(map.get("userProject"))) {
                hasUserProjectQueryParam = true;
                break;
            }
        }
        assertThat(hasUserProjectQueryParam)
                .as("presignGet must add userProject as a signed query parameter")
                .isTrue();
    }

    @Test
    void noUserProjectMeansNoUserProjectOption() {
        try (GoogleCloudStorage storage = openStorage(Optional.empty())) {
            storage.stat("a.bin");
            storage.delete("a.bin");
        }
        ArgumentCaptor<BlobGetOption[]> getCaptor = ArgumentCaptor.forClass(BlobGetOption[].class);
        verify(client).get(any(BlobId.class), getCaptor.capture());
        assertThat(getCaptor.getValue())
                .as("BlobGetOption[] must not carry a userProject when storage.gcs.user-project is unset")
                .noneMatch(opt -> opt.equals(BlobGetOption.userProject("anything")));

        ArgumentCaptor<BlobSourceOption[]> delCaptor = ArgumentCaptor.forClass(BlobSourceOption[].class);
        verify(client).delete(any(BlobId.class), delCaptor.capture());
        assertThat(delCaptor.getValue()).isEmpty();
    }

    @Test
    void detectHnsAttachesUserProjectOnConstruction() {
        try (GoogleCloudStorage ignored = openStorage(Optional.of(USER_PROJECT))) {
            // construction triggers detectHns
        }
        ArgumentCaptor<BucketGetOption[]> captor = ArgumentCaptor.forClass(BucketGetOption[].class);
        verify(client).get(any(String.class), captor.capture());
        assertThat(captor.getValue()).contains(BucketGetOption.userProject(USER_PROJECT));
    }
}
