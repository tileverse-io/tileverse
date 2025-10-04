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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@DisplayName("S3 Compatible URL Parser Tests")
class S3CompatibleUrlParserTest {

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("provideS3UrlTestCases")
    @DisplayName("Parse S3 URLs correctly")
    void shouldParseS3UrlsCorrectly(
            String description,
            String url,
            URI expectedEndpoint,
            String expectedBucket,
            String expectedKey,
            boolean shouldSucceed) {
        if (shouldSucceed) {
            S3Reference location = S3CompatibleUrlParser.parseS3Url(url);
            assertThat(location.endpoint())
                    .as("Endpoint should match for '%s'".formatted(description))
                    .isEqualTo(expectedEndpoint);
            assertThat(location.bucket())
                    .as("Bucket should match for '%s'".formatted(description))
                    .isEqualTo(expectedBucket);
            assertThat(location.key())
                    .as("Key should match for '%s'".formatted(description))
                    .isEqualTo(expectedKey);
        } else {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> S3CompatibleUrlParser.parseS3Url(url),
                    "Should throw IllegalArgumentException for invalid URL: " + url);
        }
    }

    private static Stream<Arguments> provideS3UrlTestCases() {
        return Stream.of(
                // AWS S3 URI format - uses default AWS endpoints
                Arguments.of(
                        "AWS S3 URI with nested path",
                        "s3://my-bucket/path/to/file.txt",
                        null,
                        "my-bucket",
                        "path/to/file.txt",
                        true),

                // AWS Virtual Hosted-Style URLs
                Arguments.of(
                        "AWS virtual hosted-style with region",
                        "https://my-bucket.s3.us-west-2.amazonaws.com/path/to/file.txt",
                        null,
                        "my-bucket",
                        "path/to/file.txt",
                        true),
                Arguments.of(
                        "AWS virtual hosted-style without region",
                        "https://my-bucket.s3.amazonaws.com/path/to/file.txt",
                        null,
                        "my-bucket",
                        "path/to/file.txt",
                        true),

                // AWS Path-Style URLs
                Arguments.of(
                        "AWS path-style with region",
                        "https://s3.us-west-2.amazonaws.com/my-bucket/path/to/file.txt",
                        null,
                        "my-bucket",
                        "path/to/file.txt",
                        true),
                Arguments.of(
                        "AWS path-style without region",
                        "https://s3.amazonaws.com/my-bucket/path/to/file.txt",
                        null,
                        "my-bucket",
                        "path/to/file.txt",
                        true),
                // MinIO and Local Development - custom endpoints
                Arguments.of(
                        "MinIO localhost with port",
                        "http://localhost:9000/my-bucket/path/to/file.txt",
                        URI.create("http://localhost:9000"),
                        "my-bucket",
                        "path/to/file.txt",
                        true),
                Arguments.of(
                        "MinIO custom domain",
                        "https://minio.example.com/my-bucket/path/to/file.txt",
                        URI.create("https://minio.example.com"),
                        "my-bucket",
                        "path/to/file.txt",
                        true),
                Arguments.of(
                        "MinIO IP address with port",
                        "http://192.168.1.100:9000/my-bucket/path/to/file.txt",
                        URI.create("http://192.168.1.100:9000"),
                        "my-bucket",
                        "path/to/file.txt",
                        true),

                // Other S3-Compatible Services - custom endpoints
                Arguments.of(
                        "Google Cloud Storage",
                        "https://storage.googleapis.com/my-bucket/path/to/file.txt",
                        URI.create("https://storage.googleapis.com"),
                        "my-bucket",
                        "path/to/file.txt",
                        true),
                Arguments.of(
                        "DigitalOcean Spaces",
                        "https://digitaloceanspaces.com/my-bucket/path/to/file.txt",
                        URI.create("https://digitaloceanspaces.com"),
                        "my-bucket",
                        "path/to/file.txt",
                        true),
                Arguments.of(
                        "Wasabi Cloud Storage",
                        "https://wasabisys.com/my-bucket/path/to/file.txt",
                        URI.create("https://wasabisys.com"),
                        "my-bucket",
                        "path/to/file.txt",
                        true),
                Arguments.of(
                        "Internal S3-compatible service",
                        "https://s3.company.internal/my-bucket/path/to/file.txt",
                        URI.create("https://s3.company.internal"),
                        "my-bucket",
                        "path/to/file.txt",
                        true),

                // Edge Cases - Empty Keys
                Arguments.of("S3 URI bucket root with trailing slash", "s3://my-bucket/", null, "my-bucket", "", true),
                Arguments.of(
                        "MinIO bucket root with trailing slash",
                        "http://localhost:9000/my-bucket/",
                        URI.create("http://localhost:9000"),
                        "my-bucket",
                        "",
                        true),
                Arguments.of(
                        "AWS path-style bucket root",
                        "https://s3.amazonaws.com/my-bucket/",
                        null,
                        "my-bucket",
                        "",
                        true),

                // Edge Cases - Special Characters
                Arguments.of(
                        "URL encoded spaces are decoded",
                        "https://s3.amazonaws.com/my-bucket/folder/file%20with%20spaces.txt",
                        null,
                        "my-bucket",
                        "folder/file with spaces.txt",
                        true),
                Arguments.of(
                        "URL encoded special characters",
                        "https://s3.amazonaws.com/my-bucket/path/file%2Bwith%26symbols.txt",
                        null,
                        "my-bucket",
                        "path/file+with&symbols.txt",
                        true),
                Arguments.of(
                        "Complex nested path structure",
                        "s3://my-bucket/level1/level2/level3/file.json",
                        null,
                        "my-bucket",
                        "level1/level2/level3/file.json",
                        true),
                Arguments.of(
                        "Key with special characters",
                        "http://localhost:9000/my-bucket/path/to/file-name_with.special+chars.txt",
                        URI.create("http://localhost:9000"),
                        "my-bucket",
                        "path/to/file-name_with.special+chars.txt",
                        true),

                // Edge Cases - Bucket Only
                Arguments.of(
                        "AWS path-style bucket only",
                        "https://s3.amazonaws.com/my-bucket",
                        null,
                        "my-bucket",
                        "",
                        true),
                Arguments.of(
                        "MinIO bucket only",
                        "http://localhost:9000/my-bucket",
                        URI.create("http://localhost:9000"),
                        "my-bucket",
                        "",
                        true),
                Arguments.of("S3 URI bucket only", "s3://my-bucket", null, "my-bucket", "", true),

                // Error Cases
                Arguments.of("Invalid scheme", "ftp://my-bucket/file.txt", null, null, null, false),
                Arguments.of(
                        "Empty path for HTTP URL",
                        "http://localhost:9000/",
                        URI.create("http://localhost:9000"),
                        null,
                        null,
                        true),
                Arguments.of("No bucket in path", "https://s3.amazonaws.com/", null, null, null, true));
    }
}
