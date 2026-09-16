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

import io.tileverse.storage.StorageException;
import java.io.IOException;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class EndpointEtagsTest {

    /** The message the CRT client raises for a response with no {@code ETag} header, from AWS SDK 2.41.32. */
    private static final String CRT_MESSAGE =
            "Failed to send the request: Response missing required ETag header. (SDK Attempt Count: 1)";

    private static SdkClientException crtRejection() {
        return SdkClientException.create(CRT_MESSAGE);
    }

    /** The wrapping a batched read puts around the rejection. */
    private static Throwable batchFailureChain() {
        StorageException mapped = new StorageException("Failed to read ranges from S3: " + CRT_MESSAGE, crtRejection());
        return new CompletionException(mapped);
    }

    @Test
    void anEndpointStartsWithNothingRecorded() {
        assertThat(new EndpointEtags().omitted()).isFalse();
    }

    @Test
    void anEtagOnTheResponseRecordsNothing() {
        EndpointEtags etags = new EndpointEtags();

        etags.observe("\"d41d8cd98f00b204e9800998ecf8427e\"");

        assertThat(etags.omitted()).isFalse();
    }

    @Test
    void aResponseWithoutAnEtagRecordsTheOmission() {
        EndpointEtags absent = new EndpointEtags();
        EndpointEtags blank = new EndpointEtags();

        absent.observe(null);
        blank.observe("  ");

        assertThat(absent.omitted()).isTrue();
        assertThat(blank.omitted()).isTrue();
    }

    @Test
    void aRecordedOmissionSurvivesALaterResponseWithAnEtag() {
        EndpointEtags etags = new EndpointEtags();

        etags.recordOmission();
        etags.observe("\"d41d8cd98f00b204e9800998ecf8427e\"");

        assertThat(etags.omitted()).isTrue();
    }

    @Test
    void theCrtRejectionIsRecognizedThroughItsWrappers() {
        assertThat(EndpointEtags.rejectedForMissingEtag(crtRejection())).isTrue();
        assertThat(EndpointEtags.rejectedForMissingEtag(batchFailureChain())).isTrue();
        assertThat(EndpointEtags.rejectedForMissingEtag(
                        new StorageException("read failed for: some.bin", crtRejection())))
                .isTrue();
    }

    @Test
    void otherFailuresAreNotTheMissingEtagRejection() {
        assertThat(EndpointEtags.rejectedForMissingEtag(SdkClientException.create("Unable to execute HTTP request")))
                .isFalse();
        assertThat(EndpointEtags.rejectedForMissingEtag(new CompletionException(S3Exception.builder()
                        .statusCode(404)
                        .message("Not Found")
                        .build())))
                .isFalse();
        assertThat(EndpointEtags.rejectedForMissingEtag(new IOException("connection reset")))
                .isFalse();
    }

    @Test
    void aCauseCycleDoesNotTrapTheSearch() {
        IOException first = new IOException("first");
        SdkClientException second = SdkClientException.create("second", first);
        first.initCause(second);

        assertThat(EndpointEtags.rejectedForMissingEtag(second)).isFalse();
    }
}
