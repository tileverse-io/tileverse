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

import static io.tileverse.storage.spi.StorageParameter.SUBGROUP_AUTHENTICATION;
import static java.util.function.Predicate.not;

import io.tileverse.storage.Storage;
import io.tileverse.storage.spi.AbstractStorageProvider;
import io.tileverse.storage.spi.StorageConfig;
import io.tileverse.storage.spi.StorageParameter;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * {@link StorageProvider} implementation for AWS S3.
 *
 * <p>The {@link StorageConfig#uri() URI} is used to extract the bucket and object name from an S3 URI.
 *
 * <p>An S3 URL, or Amazon Simple Storage Service Uniform Resource Locator, refers to the address used to access
 * resources stored within AWS S3. There are several forms of S3 URLs, depending on the context and desired access
 * method:
 *
 * <h2>Path-Style URLs</h2>
 *
 * <ul>
 *   <li>{@code s3://} URI: This is the canonical URI format for referencing objects within S3. It is commonly used
 *       within AWS services, tools, and libraries for internal referencing. For example:
 *       <pre>
 * {@literal s3://your-bucket-name/your-object-name}
 * </pre>
 *   <li>Public HTTP/HTTPS URLs: If an object is configured for public access, it can be accessed directly via a
 *       standard HTTP or HTTPS URL. These URLs are typically in the format:
 *       <pre>
 * {@literal https://your-bucket-name.s3.your-aws-region.amazonaws.com/your-object-name}
 * </pre>
 * </ul>
 *
 * When {@code http/s} URL schemes are used, {@link #canProcessHeaders(URI, Map)} disambiguates by checking if a header
 * starting with {@code x-amz-} was returned from the HEAD request.
 */
@Slf4j
public class S3StorageProvider extends AbstractStorageProvider {

    private final S3ClientCache clientCache = new S3ClientCache();

    /**
     * Key used as environment variable name to disable this range reader provider
     *
     * <pre>
     * {@code export IO_TILEVERSE_STORAGE_S3=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_STORAGE_S3";
    /** This range reader implementation's {@link #getId() unique identifier} */
    public static final String ID = "s3";

    /**
     * Creates a new S3RangeReaderProvider with support for caching parameters
     *
     * @see AbstractStorageProvider#MEMORY_CACHE_ENABLED
     * @see AbstractStorageProvider#MEMORY_CACHE_BLOCK_ALIGNED
     * @see AbstractStorageProvider#MEMORY_CACHE_BLOCK_SIZE
     */
    public S3StorageProvider() {
        super(true);
    }

    /**
     * Opens a {@link io.tileverse.storage.Storage} backed by the supplied sync
     * {@link software.amazon.awssdk.services.s3.S3Client}, bypassing the SPI configuration path. Useful for
     * Spring-managed clients, LocalStack/MinIO test fixtures, or custom credential-provider chains expressible only as
     * a built {@code S3Client}.
     *
     * <p><b>Capability degradation</b>: with only a sync client, {@code read} (parallel multi-part GET via the CRT
     * async client), multipart upload (via {@code S3TransferManager}), and presigned URL operations are unavailable and
     * throw {@link io.tileverse.storage.UnsupportedCapabilityException}. For full feature parity, build the matching
     * async/transfer/presigner objects and call {@link #open(java.net.URI, S3ClientBundle)} instead.
     *
     * <p>The returned {@code Storage} <b>borrows</b> the supplied client; closing the {@code Storage} does NOT close
     * the client. The caller retains ownership.
     *
     * @param uri canonical {@code s3://bucket[/prefix/]} URI; bucket parsing follows the standard S3-compatible
     *     conventions also accepted by the SPI path
     * @param client a pre-configured sync S3 client; not closed by the returned {@code Storage}
     * @return a borrowed-client {@code S3Storage} with sync-only capabilities
     */
    public static Storage open(URI uri, S3Client client) {
        if (client == null) {
            throw new IllegalArgumentException("client");
        }
        return open(uri, S3ClientBundle.syncOnly(client));
    }

    /**
     * Opens a {@link io.tileverse.storage.Storage} backed by the supplied {@link S3ClientBundle}, bypassing the SPI
     * configuration path. Use this overload when the caller wants the full feature surface (range reads, {@code read},
     * multipart upload, presigned URLs) with externally-managed SDK objects.
     *
     * <p>The returned {@code Storage} <b>borrows</b> all SDK objects in the bundle; closing the {@code Storage} does
     * NOT close them. The caller retains ownership and lifetime control.
     *
     * @param uri canonical {@code s3://bucket[/prefix/]} URI
     * @param bundle the SDK objects to use; required to have a sync client, optional async/transfer/presigner
     * @return a borrowed-client {@code S3Storage} with the capabilities reflected by the bundle's optional objects
     */
    public static Storage open(URI uri, S3ClientBundle bundle) {
        if (uri == null) {
            throw new IllegalArgumentException("uri");
        }
        if (bundle == null) {
            throw new IllegalArgumentException("bundle");
        }
        S3StorageBucketKey ref = S3StorageBucketKey.parse(uri);
        return new S3Storage(uri, ref, new BorrowedS3Handle(bundle));
    }

    /**
     * A {@link StorageParameter} to enable or disable S3 path style access. When enabled, requests will use path-style
     * addressing (e.g., {@code https://s3.amazonaws.com/bucket/key}). When disabled, virtual-hosted-style addressing
     * will be used instead (e.g., {@code https://bucket.s3.amazonaws.com/key}). This can be useful for compatibility
     * with S3-compatible storage systems that do not support virtual-hosted-style requests.
     */
    public static final StorageParameter<Boolean> S3_FORCE_PATH_STYLE = StorageParameter.builder()
            .key("storage.s3.force-path-style")
            .title("Enable S3 path style access")
            .description("""
                When enabled, requests will use path-style addressing (e.g., https://s3.amazonaws.com/bucket/key).

                When disabled, virtual-hosted-style addressing will be used instead \
                (e.g., https://bucket.s3.amazonaws.com/key).

                This can be useful for compatibility with S3-compatible storage systems that do not \
                support virtual-hosted-style requests.

                Note: When a complete S3 URL is provided, path style is automatically detected and enabled \
                for non-AWS endpoints (MinIO, Google Cloud Storage, etc.). This parameter allows explicit \
                override of the automatic detection behavior.
                """)
            .type(Boolean.class)
            .group(ID)
            .defaultValue(true)
            .build();

    /** Configuration parameter for AWS S3 region. */
    public static final StorageParameter<String> S3_REGION = StorageParameter.builder()
            .key("storage.s3.region")
            .title("Region")
            .description("""
                    Configure the region with which the SDK should communicate.

                    If this is not specified, the SDK will attempt to identify the endpoint automatically using the following logic:

                    * Check the 'aws.region' system property for the region.
                    * Check the 'AWS_REGION' environment variable for the region.
                    * Check the {user.home}/.aws/credentials and {user.home}/.aws/config files for the region.
                    * If running in EC2, check the EC2 metadata service for the region.

                    If the region is not found, an exception will be thrown.

                    Each AWS region corresponds to a separate geographical location where a set of Amazon services is deployed. These \
                    regions (except for the special `aws-global` and `aws-cn-global` regions) are separate from each other, \
                    with their own set of resources. This means a resource created in one region (eg. an SQS queue) is not available in \
                    another region.
                    """)
            .type(String.class)
            .group(ID)
            .options(Region.regions().stream()
                    // filter out global regions, S3 is inherently regional
                    .filter(not(Region::isGlobalRegion))
                    .map(Region::id)
                    .toArray())
            .build();

    /**
     * When {@code true}, build the S3 SDK client with no credential resolution at all (anonymous unsigned requests).
     * Use this for public buckets like Overture Maps' {@code overturemaps-us-west-2}. Takes precedence over access-key,
     * profile, and the default credential chain.
     */
    public static final StorageParameter<Boolean> S3_ANONYMOUS = StorageParameter.builder()
            .key("storage.s3.anonymous")
            .title("Anonymous public access")
            .description("""
                    When true, the S3 SDK client is built with the AnonymousCredentialsProvider so requests are \
                    unsigned. Use for public buckets that allow anonymous reads (e.g. Overture Maps, AWS Open Data). \
                    Takes precedence over access-key/secret, profile, and the default credential chain.
                    """)
            .type(Boolean.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .defaultValue(false)
            .build();

    /**
     * The AWS access key ID to use for authentication when both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are
     * provided.
     */
    public static final StorageParameter<String> S3_AWS_ACCESS_KEY_ID = StorageParameter.builder()
            .key("storage.s3.aws-access-key-id")
            .title("AWS Access Key ID")
            .description("""
                    The AWS access key ID to use for authentication.

                    This parameter must be used together with AWS_SECRET_ACCESS_KEY. When both are provided, \
                    they will be used for authentication regardless of the USE_DEFAULT_CREDENTIALS_PROVIDER setting.

                    If neither AWS_ACCESS_KEY_ID nor AWS_SECRET_ACCESS_KEY are provided, authentication behavior \
                    is controlled by the USE_DEFAULT_CREDENTIALS_PROVIDER parameter.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /**
     * The AWS secret access key to use for authentication when both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are
     * provided.
     */
    public static final StorageParameter<String> S3_AWS_SECRET_ACCESS_KEY = StorageParameter.builder()
            .key("storage.s3.aws-secret-access-key")
            .title("AWS Secret Access Key")
            .description("""
                    The AWS secret access key to use for authentication.

                    This parameter must be used together with AWS_ACCESS_KEY_ID. When both are provided, \
                    they will be used for authentication regardless of the USE_DEFAULT_CREDENTIALS_PROVIDER setting.

                    If neither AWS_ACCESS_KEY_ID nor AWS_SECRET_ACCESS_KEY are provided, authentication behavior \
                    is controlled by the USE_DEFAULT_CREDENTIALS_PROVIDER parameter.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /** Configuration parameter to control whether to use the default AWS credentials provider chain. */
    public static final StorageParameter<Boolean> S3_USE_DEFAULT_CREDENTIALS_PROVIDER = StorageParameter.builder()
            .key("storage.s3.use-default-credentials-provider")
            .title("Use Default Credentials Provider")
            .description("""
                    When enabled, the AWS default credentials provider chain is used, which looks for credentials \
                    in this order:
                      1. Java System Properties - aws.accessKeyId and aws.secretAccessKey
                      2. Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
                      3. Web Identity Token File - from the path specified in the AWS_WEB_IDENTITY_TOKEN_FILE environment variable
                      4. Shared Credentials File - at ~/.aws/credentials
                      5. Amazon ECS Container Credentials - loaded from the endpoint specified in the AWS_CONTAINER_CREDENTIALS_RELATIVE_URI environment variable
                      6. Amazon EC2 Instance Profile Credentials - loaded from the Amazon EC2 metadata service

                    If neither default credentials provider or access/secret key are used, annonymous access will \
                    be attempted.
                    """)
            .type(Boolean.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /** Configuration parameter to specify a custom AWS credentials profile name. */
    public static final StorageParameter<String> S3_DEFAULT_CREDENTIALS_PROFILE = StorageParameter.builder()
            .key("storage.s3.default-credentials-profile")
            .title("Default Credentials Profile")
            .description("""
                    The AWS credentials profile name to use when USE_DEFAULT_CREDENTIALS_PROVIDER is enabled.

                    If not specified, the 'default' profile is used. This parameter is only effective when \
                    USE_DEFAULT_CREDENTIALS_PROVIDER is set to true.

                    The profile should exist in the AWS credentials file (typically ~/.aws/credentials) or \
                    AWS config file (typically ~/.aws/config).
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    static final List<StorageParameter<?>> PARAMS = List.of(
            S3_FORCE_PATH_STYLE,
            S3_REGION,
            S3_ANONYMOUS,
            S3_AWS_ACCESS_KEY_ID,
            S3_AWS_SECRET_ACCESS_KEY,
            S3_USE_DEFAULT_CREDENTIALS_PROVIDER,
            S3_DEFAULT_CREDENTIALS_PROFILE);

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean isAvailable() {
        return StorageProvider.isEnabled(ENABLED_KEY);
    }

    @Override
    public String getDescription() {
        return "AWS S3 (and S3-compatible) provider.";
    }

    @Override
    public int getOrder() {
        return -100; // High priority
    }

    @Override
    protected List<StorageParameter<?>> buildParameters() {
        return PARAMS;
    }

    @Override
    public boolean canProcess(StorageConfig config) {
        if (StorageConfig.matches(config, getId(), "s3", "http", "https")) {
            try {
                URI uri = config.uri();
                S3Reference l = S3CompatibleUrlParser.parseS3Url(uri);

                boolean hasValidBucket =
                        l.bucket() != null && !l.bucket().trim().isEmpty();
                if (!hasValidBucket) {
                    log.debug("Skipping URL {} - no bucket parsed", uri);
                    return false;
                }

                String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
                // For s3:// / s3a://, bucket is sufficient (Storage use case includes bucket-only URIs).
                // For http(s)://, also require a key so that ambiguous custom-domain URLs fall back to HTTP.
                if (scheme.equals("s3") || scheme.equals("s3a")) {
                    return true;
                }
                boolean hasValidKey = l.key() != null && !l.key().trim().isEmpty();
                if (!hasValidKey) {
                    log.debug("Skipping HTTP URL {} - no key parsed", uri);
                    return false;
                }
                return true;
            } catch (IllegalArgumentException e) {
                log.debug("Can't process URL {}: {}", config.uri(), e.getMessage());
            }
        }
        return false;
    }

    @Override
    public boolean canProcessHeaders(URI uri, Map<String, List<String>> headers) {
        Set<String> headerNames = headers.keySet();
        return headerNames.stream().anyMatch("x-amz-request-id"::equalsIgnoreCase);
    }

    @Override
    public Storage createStorage(StorageConfig config) {
        URI uri = config.uri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.uri() is required for S3Storage");
        }
        S3StorageBucketKey ref = S3StorageBucketKey.parse(uri);
        S3ClientCache.Lease lease = clientCache.acquire(keyFor(config));
        return new S3Storage(uri, ref, lease);
    }

    /**
     * Resolves the {@link S3ClientCache.Key} cache discriminator for the given config. Package-private so unit tests
     * can exercise region/credentials/endpoint precedence without going through {@link #createStorage} (which would
     * also acquire a real SDK client lease).
     *
     * <p>Region resolution order:
     *
     * <ol>
     *   <li>explicit {@code storage.s3.region} in {@code StorageConfig}
     *   <li>region parsed from the URI itself (e.g. {@code *.s3.us-west-2.amazonaws.com})
     *   <li>fallback to {@code us-east-1}
     * </ol>
     */
    static S3ClientCache.Key keyFor(StorageConfig config) {
        URI uri = config.uri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.uri() is required for S3Storage");
        }
        S3Reference s3Ref = S3CompatibleUrlParser.parseS3Url(uri);
        String region = config.getParameter(S3_REGION)
                .filter(s -> !s.isBlank())
                .orElseGet(() -> s3Ref.region() != null && !s3Ref.region().isBlank() ? s3Ref.region() : "us-east-1");
        boolean anonymous = config.getParameter(S3_ANONYMOUS).orElse(false);
        Optional<String> accessKey = config.getParameter(S3_AWS_ACCESS_KEY_ID);
        Optional<String> secretKey = config.getParameter(S3_AWS_SECRET_ACCESS_KEY);
        Optional<String> profile = config.getParameter(S3_DEFAULT_CREDENTIALS_PROFILE);
        boolean forcePathStyle = config.getParameter(S3_FORCE_PATH_STYLE).orElse(false);
        Optional<URI> endpointOverride = s3Ref.endpointOverride();
        return new S3ClientCache.Key(
                region, endpointOverride, anonymous, accessKey, secretKey, profile, forcePathStyle);
    }
}
