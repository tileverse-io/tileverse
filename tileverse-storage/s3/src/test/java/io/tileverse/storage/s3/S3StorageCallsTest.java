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
package io.tileverse.storage.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.DeleteResult;
import io.tileverse.storage.ListOptions;
import io.tileverse.storage.PresignWriteOptions;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.StorageCapabilities;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import io.tileverse.storage.UnsupportedCapabilityException;
import io.tileverse.storage.WriteOptions;
import java.io.InterruptedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

/**
 * Operations of {@link S3Storage} over a mocked async client: how failures and interrupts reach the caller, how a
 * listing pages, and which operations open the transfer manager or the presigner.
 */
@ExtendWith(MockitoExtension.class)
class S3StorageCallsTest {

    private static final URI BASE_URI = URI.create("s3://bucket/");

    @Mock
    private S3AsyncClient client;

    @Mock
    private S3ClientHandle handle;

    private S3Storage storage;

    @BeforeEach
    void openStorage() {
        lenient().when(handle.client()).thenReturn(client);
        // a mock has no client configuration to report: the readers identify as s3:// URIs
        lenient().when(client.serviceClientConfiguration()).thenThrow(new UnsupportedOperationException());
        storage = new S3Storage(BASE_URI, S3StorageBucketKey.parse(BASE_URI), handle, false);
    }

    @Test
    void aMissingKeyStatsEmpty() {
        when(client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        NoSuchKeyException.builder().message("missing").build()));

        assertThat(storage.stat("missing.bin")).isEmpty();
    }

    @Test
    void aClientSideFailureOfStatArrivesAsAStorageException() {
        SdkClientException refused = SdkClientException.create("connection refused");
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(CompletableFuture.failedFuture(refused));

        assertThatThrownBy(() -> storage.stat("object.bin"))
                .isExactlyInstanceOf(StorageException.class)
                .hasMessageContaining("object.bin")
                .cause()
                .isSameAs(refused);
    }

    @Test
    void anInterruptedStatFailsAndKeepsTheFlagSet() throws Exception {
        CompletableFuture<HeadObjectResponse> neverAnswered = new CompletableFuture<>();
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(neverAnswered);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> waiting = new AtomicReference<>();
        try {
            Future<Throwable> outcome = caller.submit(() -> {
                waiting.set(Thread.currentThread());
                try {
                    storage.stat("object.bin");
                    return null;
                } catch (StorageException failure) {
                    return Thread.interrupted() ? failure : new AssertionError("interrupt flag cleared", failure);
                }
            });
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> verify(client).headObject(any(HeadObjectRequest.class)));

            waiting.get().interrupt();

            assertThat(outcome.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(StorageException.class)
                    .hasCauseInstanceOf(InterruptedIOException.class);
            assertThat(neverAnswered).isCancelled();
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    @Timeout(10) // an endless listing fails the test instead of hanging the build
    void aListingFollowsTheContinuationToken() {
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(invocation -> {
            ListObjectsV2Request request = invocation.getArgument(0);
            boolean firstPage = request.continuationToken() == null;
            ListObjectsV2Response page =
                    firstPage ? page(true, "next-page", "a.bin", "b.bin") : page(false, null, "c.bin");
            return CompletableFuture.completedFuture(page);
        });

        List<String> keys;
        try (Stream<StorageEntry> listing =
                storage.list("**", ListOptions.builder().pageSize(2).build())) {
            keys = listing.map(StorageEntry::key).toList();
        }

        assertThat(keys).containsExactly("a.bin", "b.bin", "c.bin");
        ArgumentCaptor<ListObjectsV2Request> sent = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(client, times(2)).listObjectsV2(sent.capture());
        assertThat(sent.getAllValues().get(1).continuationToken()).isEqualTo("next-page");
        assertThat(sent.getAllValues())
                .allSatisfy(request -> assertThat(request.maxKeys()).isEqualTo(2));
    }

    @Test
    @Timeout(10) // an endless listing fails the test instead of hanging the build
    void aTokenIsFollowedOnAPageNotMarkedTruncated() {
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(invocation -> {
            ListObjectsV2Request request = invocation.getArgument(0);
            boolean firstPage = request.continuationToken() == null;
            ListObjectsV2Response page = firstPage ? page(false, "next-page", "a.bin") : page(false, null, "b.bin");
            return CompletableFuture.completedFuture(page);
        });

        List<String> keys;
        try (Stream<StorageEntry> listing = storage.list("**")) {
            keys = listing.map(StorageEntry::key).toList();
        }

        assertThat(keys).containsExactly("a.bin", "b.bin");
        ArgumentCaptor<ListObjectsV2Request> sent = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(client, times(2)).listObjectsV2(sent.capture());
        assertThat(sent.getAllValues().get(1).continuationToken()).isEqualTo("next-page");
    }

    @Test
    @Timeout(10) // an endless listing fails the test instead of hanging the build
    void aTruncatedPageWithoutATokenEndsTheListing() {
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(CompletableFuture.completedFuture(page(true, null, "a.bin")));

        try (Stream<StorageEntry> listing = storage.list("**")) {
            assertThat(listing.map(StorageEntry::key)).containsExactly("a.bin");
        }
        verify(client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    void anInterruptedListingFailsAndKeepsTheFlagSet() throws Exception {
        CompletableFuture<ListObjectsV2Response> neverAnswered = new CompletableFuture<>();
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(neverAnswered);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> waiting = new AtomicReference<>();
        try {
            Future<Throwable> outcome = caller.submit(() -> {
                waiting.set(Thread.currentThread());
                try (Stream<StorageEntry> listing = storage.list("**")) {
                    return new AssertionError("listed " + listing.count() + " entries");
                } catch (StorageException failure) {
                    return Thread.interrupted() ? failure : new AssertionError("interrupt flag cleared", failure);
                }
            });
            await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> verify(client).listObjectsV2(any(ListObjectsV2Request.class)));

            waiting.get().interrupt();

            assertThat(outcome.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(StorageException.class)
                    .hasCauseInstanceOf(InterruptedIOException.class);
            assertThat(neverAnswered).isCancelled();
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    void aFailingFirstPageFailsTheListCallItself() {
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(CompletableFuture.failedFuture(serviceFailure(403)));

        assertThatThrownBy(() -> storage.list("**")).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void anInterruptedReadAbortsTheStreamArrivingDespiteTheCancel() {
        ResponseInputStream<GetObjectResponse> lateStream = mock(ResponseInputStream.class);
        CompletableFuture<ResponseInputStream<GetObjectResponse>> answeredAtTheCancel = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                complete(lateStream);
                return false;
            }
        };
        when(client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
                .thenAnswer(invocation -> {
                    // the interrupt lands after the request went out, before the response arrives
                    Thread.currentThread().interrupt();
                    return answeredAtTheCancel;
                });
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<ReadHandle> read = caller.submit(() -> storage.read("object.bin"));

            assertThatThrownBy(() -> read.get(5, TimeUnit.SECONDS))
                    .cause()
                    .isInstanceOf(StorageException.class)
                    .hasCauseInstanceOf(InterruptedIOException.class);
            verify(lateStream).abort();
        } finally {
            caller.shutdownNow();
        }
    }

    @Test
    void aServiceFailureOfADeleteBatchFailsItsKeysAndTheNextBatchRuns() {
        List<String> keys =
                IntStream.range(0, 1001).mapToObj(i -> "k" + i + ".bin").toList();
        DeleteObjectsResponse secondBatch = DeleteObjectsResponse.builder()
                .deleted(DeletedObject.builder().key("k1000.bin").build())
                .build();
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(serviceFailure(503)))
                .thenReturn(CompletableFuture.completedFuture(secondBatch));

        DeleteResult result = storage.deleteAll(keys);

        assertThat(result.failed()).hasSize(1000);
        assertThat(result.failed().get("k0.bin"))
                .isInstanceOf(TransientStorageException.class)
                .hasMessageContaining("k0.bin");
        assertThat(result.deleted()).containsExactly("k1000.bin");
    }

    @Test
    void aClientSideFailureOfADeleteBatchEndsTheCall() {
        when(client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(SdkClientException.create("connection reset")));
        List<String> keys = List.of("a.bin", "b.bin");

        assertThatThrownBy(() -> storage.deleteAll(keys)).isExactlyInstanceOf(StorageException.class);
    }

    @Test
    void aMultipartUploadGoesThroughTheTransferManager(@TempDir Path dir) throws Exception {
        S3TransferManager transferManager = mock(S3TransferManager.class);
        FileUpload upload = mock(FileUpload.class);
        CompletedFileUpload completed = CompletedFileUpload.builder()
                .response(PutObjectResponse.builder().build())
                .build();
        when(handle.transferManager()).thenReturn(transferManager);
        when(transferManager.uploadFile(any(UploadFileRequest.class))).thenReturn(upload);
        when(upload.completionFuture()).thenReturn(CompletableFuture.completedFuture(completed));
        stubStat(10);
        Path source = Files.write(dir.resolve("source.bin"), new byte[10]);

        storage.put("object.bin", source, WriteOptions.defaults());

        verify(transferManager).uploadFile(any(UploadFileRequest.class));
        verify(client, never()).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
    }

    @Test
    void aTransferManagerFailingToOpenArrivesAsItself(@TempDir Path dir) throws Exception {
        IllegalStateException closed = new IllegalStateException("S3 Storage is closed");
        when(handle.transferManager()).thenThrow(closed);
        Path source = Files.write(dir.resolve("source.bin"), new byte[10]);
        WriteOptions inParts = WriteOptions.defaults();

        assertThatThrownBy(() -> storage.put("object.bin", source, inParts)).isSameAs(closed);
    }

    @Test
    void anUploadWithMultipartDisabledIsOnePutObject(@TempDir Path dir) throws Exception {
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        PutObjectResponse.builder().build()));
        stubStat(10);
        Path source = Files.write(dir.resolve("source.bin"), new byte[10]);
        WriteOptions singleRequest =
                WriteOptions.builder().disableMultipart(true).build();

        storage.put("object.bin", source, singleRequest);

        verify(client).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        verify(handle, never()).transferManager();
    }

    @Test
    void operationsOtherThanMultipartUploadsAndPresignsBuildNeither() throws Exception {
        when(client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        PutObjectResponse.builder().build()));
        when(client.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        CopyObjectResponse.builder().build()));
        when(client.deleteObject(any(DeleteObjectRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(
                        DeleteObjectResponse.builder().build()));
        stubStat(3);

        storage.put("a.bin", new byte[] {1, 2, 3});
        storage.copy("a.bin", "b.bin", CopyOptions.defaults());
        storage.delete("a.bin");
        storage.openRangeReader("b.bin").close();

        verify(handle, never()).transferManager();
        verify(handle, never()).presigner();
    }

    @Test
    void presigningWithoutAPresignerIsUnsupported() {
        when(handle.presigner()).thenReturn(Optional.empty());
        Duration ttl = Duration.ofMinutes(5);

        assertThatThrownBy(() -> storage.presignGet("object.bin", ttl))
                .isInstanceOf(UnsupportedCapabilityException.class);
    }

    @Test
    void aStorageOverAHandleWithoutAPresignerCannotPresign() {
        when(handle.presigns()).thenReturn(false);

        StorageCapabilities capabilities = storageOverTheStubbedHandle().capabilities();

        assertThat(capabilities.presignedUrls()).isFalse();
        assertThat(capabilities.maxPresignTtl()).isEmpty();
    }

    @Test
    void aClientSideFailureOfPresignGetArrivesAsAStorageException() {
        SdkClientException noCredentials = SdkClientException.create("Unable to load credentials");
        S3Presigner presigner = mock(S3Presigner.class);
        when(presigner.presignGetObject(any(GetObjectPresignRequest.class))).thenThrow(noCredentials);
        S3Storage presigning = storageOverAPresigningHandle(presigner);
        Duration ttl = Duration.ofMinutes(5);

        assertThatThrownBy(() -> presigning.presignGet("object.bin", ttl))
                .isExactlyInstanceOf(StorageException.class)
                .hasMessageContaining("object.bin")
                .cause()
                .isSameAs(noCredentials);
    }

    @Test
    void aClientSideFailureOfPresignPutArrivesAsAStorageException() {
        SdkClientException noCredentials = SdkClientException.create("Unable to load credentials");
        S3Presigner presigner = mock(S3Presigner.class);
        when(presigner.presignPutObject(any(PutObjectPresignRequest.class))).thenThrow(noCredentials);
        S3Storage presigning = storageOverAPresigningHandle(presigner);
        Duration ttl = Duration.ofMinutes(5);
        PresignWriteOptions options = PresignWriteOptions.defaults();

        assertThatThrownBy(() -> presigning.presignPut("object.bin", ttl, options))
                .isExactlyInstanceOf(StorageException.class)
                .hasMessageContaining("object.bin")
                .cause()
                .isSameAs(noCredentials);
    }

    private S3Storage storageOverAPresigningHandle(S3Presigner presigner) {
        when(handle.presigns()).thenReturn(true);
        when(handle.presigner()).thenReturn(Optional.of(presigner));
        return storageOverTheStubbedHandle();
    }

    /** A Storage built after the stubs of the test: {@link #openStorage()} built its Storage before them. */
    private S3Storage storageOverTheStubbedHandle() {
        return new S3Storage(BASE_URI, S3StorageBucketKey.parse(BASE_URI), handle, false);
    }

    private void stubStat(long size) {
        HeadObjectResponse head = HeadObjectResponse.builder()
                .contentLength(size)
                .lastModified(Instant.EPOCH)
                .build();
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(CompletableFuture.completedFuture(head));
    }

    private static ListObjectsV2Response page(boolean truncated, String nextToken, String... keys) {
        List<S3Object> contents = Arrays.stream(keys)
                .map(key -> S3Object.builder()
                        .key(key)
                        .size(1L)
                        .lastModified(Instant.EPOCH)
                        .build())
                .toList();
        return ListObjectsV2Response.builder()
                .isTruncated(truncated)
                .nextContinuationToken(nextToken)
                .contents(contents)
                .build();
    }

    private static S3Exception serviceFailure(int status) {
        // S3Exception.Builder inherits build() from AwsServiceException.Builder: the static type needs the cast
        return (S3Exception) S3Exception.builder()
                .statusCode(status)
                .message("status " + status)
                .build();
    }
}
