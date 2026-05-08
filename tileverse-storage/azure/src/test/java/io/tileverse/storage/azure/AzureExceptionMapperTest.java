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
package io.tileverse.storage.azure;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import io.tileverse.storage.AccessDeniedException;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.PreconditionFailedException;
import io.tileverse.storage.RangeNotSatisfiableException;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.TransientStorageException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class AzureExceptionMapperTest {

    @Test
    void status404MapsToNotFound() {
        StorageException mapped = AzureExceptionMapper.map(httpException(404, "BlobNotFound"), "k");
        assertThat(mapped).isInstanceOf(NotFoundException.class);
    }

    @Test
    void status403MapsToAccessDenied() {
        StorageException mapped = AzureExceptionMapper.map(httpException(403, "AuthorizationFailure"), "k");
        assertThat(mapped).isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void status412MapsToPreconditionFailed() {
        StorageException mapped = AzureExceptionMapper.map(httpException(412, "ConditionNotMet"), "k");
        assertThat(mapped).isInstanceOf(PreconditionFailedException.class);
    }

    @Test
    void status416MapsToRangeNotSatisfiable() {
        StorageException mapped = AzureExceptionMapper.map(httpException(416, "InvalidRange"), "k");
        assertThat(mapped).isInstanceOf(RangeNotSatisfiableException.class);
    }

    @Test
    void status500MapsToTransient() {
        StorageException mapped = AzureExceptionMapper.map(httpException(500, "InternalError"), "k");
        assertThat(mapped).isInstanceOf(TransientStorageException.class);
    }

    @Test
    void status429MapsToTransient() {
        StorageException mapped = AzureExceptionMapper.map(httpException(429, "TooManyRequests"), "k");
        assertThat(mapped).isInstanceOf(TransientStorageException.class);
    }

    @Test
    void unmappedStatusFallsBackToStorageException() {
        StorageException mapped = AzureExceptionMapper.map(httpException(418, "Teapot"), "k");
        assertThat(mapped).isInstanceOf(StorageException.class).isNotInstanceOf(NotFoundException.class);
    }

    private static HttpResponseException httpException(int status, String errorCode) {
        HttpRequest req = new HttpRequest(com.azure.core.http.HttpMethod.GET, "https://example.com/blob");
        HttpResponse resp = new StubHttpResponse(req, status);
        return new HttpResponseException("Azure error: " + errorCode, resp);
    }

    private static final class StubHttpResponse extends HttpResponse {
        private final int status;

        StubHttpResponse(HttpRequest req, int status) {
            super(req);
            this.status = status;
        }

        @Override
        public int getStatusCode() {
            return status;
        }

        @Override
        public String getHeaderValue(String name) {
            return null;
        }

        @Override
        public HttpHeaders getHeaders() {
            return new HttpHeaders();
        }

        @Override
        public Flux<java.nio.ByteBuffer> getBody() {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray() {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString() {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString(java.nio.charset.Charset charset) {
            return Mono.empty();
        }
    }
}
