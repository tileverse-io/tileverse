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

import java.net.URI;
import java.util.Optional;

/**
 * Represents the components of an S3-compatible URL.
 *
 * @param endpoint the service endpoint URI (null for AWS S3 URIs using default endpoints)
 * @param bucket the bucket name
 * @param key the object key (empty string for bucket root)
 * @param region the AWS region (null if unknown or not applicable)
 */
record S3Reference(URI endpoint, String bucket, String key, String region) {

    S3Reference() {
        this(null, null, null, null);
    }
    /**
     * Returns true if this location uses the default AWS S3 endpoint.
     */
    public boolean isDefaultAwsEndpoint() {
        return endpoint == null;
    }

    /**
     * Returns true if this location requires path-style access.
     * HTTP/HTTPS URIs require path-style access.
     */
    public boolean requiresPathStyle() {
        // If we have an endpoint, it came from an HTTP/HTTPS URI, so use path-style
        return endpoint != null;
    }

    public Optional<URI> endpointOverride() {
        return Optional.ofNullable(endpoint);
    }

    @Override
    public String toString() {
        if (isDefaultAwsEndpoint()) {
            return "s3://%s/%s".formatted(bucket, key);
        }
        return "%s/%s/%s".formatted(endpoint, bucket, key);
    }

    S3Reference withEndpoint(URI endpoint) {
        return new S3Reference(endpoint, bucket(), key(), region());
    }

    S3Reference withBucket(String bucket) {
        return new S3Reference(endpoint(), bucket, key(), region());
    }

    S3Reference withKey(String key) {
        return new S3Reference(endpoint(), bucket(), key, region());
    }

    S3Reference withRegion(String region) {
        return new S3Reference(endpoint(), bucket(), key(), region);
    }
}
