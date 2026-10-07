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

import static io.tileverse.storage.StorageParameter.SUBGROUP_AUTHENTICATION;
import static java.util.function.Predicate.not;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.batch.BatchProviderHelper;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.spi.AbstractStorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * {@link StorageProvider} implementation for AWS S3.
 *
 * <p>The {@link StorageConfig#baseUri() URI} is used to extract the bucket and object name from an S3 URI.
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
 * <p>The {@code s3://} form selects this provider by itself. An {@code http(s)} URL is served only when the config
 * names the provider ({@code storage.provider=s3}); the endpoint then comes from the URL.
 */
@Slf4j
public class S3StorageProvider extends AbstractStorageProvider {

    private final S3ClientCache clientCache;

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

    /**
     * A {@link StorageParameter} to enable Requester Pays for the bucket. When {@code true}, every request adds the
     * {@code x-amz-request-payer: requester} header so that the requester (not the bucket owner) is billed for egress
     * and operations. Required to access buckets configured with the Requester Pays billing model (e.g.
     * {@code s3://noaa-nexrad-level2/}); without it those buckets reject reads with {@code 403 Forbidden}.
     *
     * <p><b>Authentication is mandatory.</b> AWS rejects anonymous (unsigned) requests against Requester Pays buckets
     * unconditionally; AWS uses the signed identity to determine the account to bill. Combine this flag with real
     * credentials (default credential chain, named profile, or static access/secret key) and do <i>not</i> combine it
     * with {@link #S3_ANONYMOUS}.
     */
    public static final StorageParameter<Boolean> S3_REQUESTER_PAYS = StorageParameter.builder()
            .key("storage.s3.requester-pays")
            .title("Requester Pays bucket")
            .description("""
                    When enabled, every S3 request adds the x-amz-request-payer: requester header so that \
                    the requester (rather than the bucket owner) is billed for egress and per-operation costs.

                    Required to access buckets configured with the Requester Pays billing model. Without the \
                    header, such buckets respond with 403 Forbidden. Examples include several AWS Open Data \
                    and NOAA datasets (e.g. s3://noaa-nexrad-level2/).

                    Authentication is mandatory: AWS rejects anonymous (unsigned) requests against Requester \
                    Pays buckets, since the signed identity is what AWS uses to determine the billed account. \
                    Combine this flag with real credentials (default credential chain, named profile, or static \
                    access/secret key) and do not combine it with storage.s3.anonymous=true.

                    The flag has no effect against regular buckets; the header is silently ignored.
                    """)
            .type(Boolean.class)
            .group(ID)
            .defaultValue(false)
            .build();

    /**
     * A {@link StorageParameter} to override the S3 service endpoint, letting a canonical {@code s3://bucket/key} URI
     * target a non-AWS S3-compatible service (MinIO, Ceph, Cloudflare R2, DigitalOcean Spaces, Wasabi, LocalStack)
     * without encoding the service host into the URI.
     *
     * <p>When set, this takes precedence over any endpoint inferred from an {@code http(s)://host/bucket/key} base URI.
     * Because custom endpoints almost always require path-style addressing, setting an endpoint defaults
     * {@link #S3_FORCE_PATH_STYLE} to {@code true}; set {@code storage.s3.force-path-style} explicitly to override.
     */
    public static final StorageParameter<URI> S3_ENDPOINT = StorageParameter.builder()
            .key("storage.s3.endpoint")
            .title("Service endpoint")
            .description("""
                    Override the S3 service endpoint to let a canonical s3://bucket/key URI target a non-AWS \
                    S3-compatible service (e.g. http://localhost:9000 for MinIO, https://<account>.r2.cloudflarestorage.com \
                    for Cloudflare R2, https://<region>.digitaloceanspaces.com for DigitalOcean Spaces).

                    The value is the service root (scheme, host, and optional port) without a bucket or key, \
                    for example http://localhost:9000.

                    When set, it takes precedence over any endpoint inferred from an http(s)://host/bucket/key base URI. \
                    Leave it unset to use the default AWS endpoints for the configured region.

                    Because custom endpoints almost always require path-style addressing, setting an endpoint \
                    defaults storage.s3.force-path-style to true. Set storage.s3.force-path-style explicitly to override.
                    """)
            .type(URI.class)
            .group(ID)
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
                    The AWS access key ID used to sign requests, together with the secret access key; \
                    either key alone is ignored. Anonymous access takes precedence over the keys.

                    Without the keys, requests use the credentials profile when one is named, and the AWS \
                    default credentials provider chain otherwise.
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
                    The AWS secret access key used to sign requests, together with the access key ID; \
                    either key alone is ignored. Anonymous access takes precedence over the keys.

                    Without the keys, requests use the credentials profile when one is named, and the AWS \
                    default credentials provider chain otherwise.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /** Configuration parameter to specify a custom AWS credentials profile name. */
    public static final StorageParameter<String> S3_DEFAULT_CREDENTIALS_PROFILE = StorageParameter.builder()
            .key("storage.s3.default-credentials-profile")
            .title("Default Credentials Profile")
            .description("""
                    The name of a profile in the AWS credentials file (typically ~/.aws/credentials) or AWS \
                    config file (typically ~/.aws/config). Requests use only that profile, whatever the \
                    environment sets. Anonymous access and the access keys take precedence over it.

                    Without a profile, the access keys, or anonymous access, the AWS default credentials provider \
                    chain resolves the credentials: system properties, environment variables, a web identity \
                    token, the profile named by AWS_PROFILE (or 'default'), container credentials, then the EC2 \
                    instance profile.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    static final List<StorageParameter<?>> PARAMS = List.of(
            S3_FORCE_PATH_STYLE,
            S3_REQUESTER_PAYS,
            S3_ENDPOINT,
            S3_REGION,
            S3_ANONYMOUS,
            S3_AWS_ACCESS_KEY_ID,
            S3_AWS_SECRET_ACCESS_KEY,
            S3_DEFAULT_CREDENTIALS_PROFILE);

    /**
     * Creates a new S3RangeReaderProvider with support for caching parameters
     *
     * @see AbstractStorageProvider#MEMORY_CACHE_ENABLED
     */
    public S3StorageProvider() {
        this(S3ClientCache.INSTANCE);
    }

    /** Creates a provider leasing its clients from {@code clientCache}; tests inspect the cache after an open. */
    S3StorageProvider(S3ClientCache clientCache) {
        super(true);
        this.clientCache = Objects.requireNonNull(clientCache, "clientCache");
    }

    /** Returns the cache leasing the clients of the storages created by this provider. */
    S3ClientCache clientCache() {
        return clientCache;
    }

    /**
     * Opens a {@link Storage} over an {@link S3AsyncClient} built by the caller. It serves tests, and client
     * configuration with no {@code storage.s3.*} parameter yet.
     *
     * <p>The {@code Storage} uses the client as built and never closes it. Multipart uploads go through a transfer
     * manager built at the first one and closed with the {@code Storage}; they run in parts only on a multipart-enabled
     * or CRT-based client. Presigned URLs throw {@link io.tileverse.storage.UnsupportedCapabilityException}: they need
     * a {@code Storage} opened from a URI or a {@link StorageConfig}.
     *
     * <p>A multipart-enabled client also downloads whole objects part by part and copies large objects in parts. A
     * CRT-based client fails on S3-compatible endpoints omitting the {@code ETag} header.
     *
     * @param uri canonical {@code s3://bucket[/prefix/]} URI
     * @param client the client serving the operations; never closed by the returned {@code Storage}
     * @return a {@code Storage} over the caller's client
     */
    public static Storage open(URI uri, S3AsyncClient client) {
        if (uri == null) {
            throw new IllegalArgumentException("uri");
        }
        if (client == null) {
            throw new IllegalArgumentException("client");
        }
        S3StorageBucketKey ref = S3StorageBucketKey.parse(uri);
        return new S3Storage(uri, ref, new BorrowedS3Handle(client), false);
    }

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
        return BatchProviderHelper.withBatchParameters(PARAMS);
    }

    @Override
    public boolean canProcess(StorageConfig config) {
        if (matches(config, "s3")) {
            Optional<S3Reference> reference = parse(config.baseUri());
            return reference.filter(S3StorageProvider::hasBucket).isPresent();
        }
        boolean namedHttpUrl = matches(config, "http", "https") && isNamedBy(config);
        if (!namedHttpUrl) {
            return false;
        }
        Optional<S3Reference> reference = parse(config.baseUri());
        return reference.filter(S3StorageProvider::hasBucketAndKey).isPresent();
    }

    /** A Storage can be rooted at a bucket: an {@code s3://} URI needs no key. */
    private static boolean hasBucket(S3Reference reference) {
        return isPresent(reference.bucket());
    }

    /** An {@code http(s)} URL with no key is a service endpoint or a bucket root, and is left unclaimed. */
    private static boolean hasBucketAndKey(S3Reference reference) {
        return hasBucket(reference) && isPresent(reference.key());
    }

    private static boolean isPresent(String part) {
        return part != null && !part.isBlank();
    }

    private static Optional<S3Reference> parse(URI uri) {
        try {
            return Optional.of(S3CompatibleUrlParser.parseS3Url(uri));
        } catch (IllegalArgumentException e) {
            log.debug("Can't process URL {}: {}", uri, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public Storage createStorage(StorageConfig config) {
        URI uri = config.baseUri();
        S3StorageBucketKey ref = S3StorageBucketKey.parse(uri);
        boolean requesterPays = config.getParameter(S3_REQUESTER_PAYS).orElse(false);
        BatchSettings batchSettings = BatchProviderHelper.objectStoreSettings(config);
        // acquired after resolving the settings: an invalid one must fail the open before a client set is leased
        S3ClientCache.Lease lease = clientCache.acquire(keyFor(config));
        return new S3Storage(uri, ref, lease, requesterPays, batchSettings);
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
     *
     * <p>Endpoint resolution order:
     *
     * <ol>
     *   <li>explicit {@code storage.s3.endpoint} in {@code StorageConfig}
     *   <li>endpoint parsed from an {@code http(s)://host/bucket/key} base URI
     *   <li>none (default AWS endpoints for the resolved region)
     * </ol>
     *
     * <p>{@code storage.s3.force-path-style} defaults to {@code true} whenever an endpoint override is in effect (from
     * either source) and {@code false} for canonical AWS URIs; an explicit value always wins.
     */
    static S3ClientCache.Key keyFor(StorageConfig config) {
        URI uri = config.baseUri();
        S3Reference s3Ref = S3CompatibleUrlParser.parseS3Url(uri);
        String region = config.getParameter(S3_REGION)
                .filter(s -> !s.isBlank())
                .orElseGet(() -> s3Ref.region() != null && !s3Ref.region().isBlank() ? s3Ref.region() : "us-east-1");
        boolean anonymous = config.getParameter(S3_ANONYMOUS).orElse(false);
        Optional<String> accessKey = config.getParameter(S3_AWS_ACCESS_KEY_ID);
        Optional<String> secretKey = config.getParameter(S3_AWS_SECRET_ACCESS_KEY);
        Optional<String> profile = config.getParameter(S3_DEFAULT_CREDENTIALS_PROFILE);
        Optional<URI> endpointOverride = config.getParameter(S3_ENDPOINT).or(s3Ref::endpointOverride);
        boolean forcePathStyle = config.getParameter(S3_FORCE_PATH_STYLE).orElse(endpointOverride.isPresent());
        return new S3ClientCache.Key(
                region, endpointOverride, anonymous, accessKey, secretKey, profile, forcePathStyle);
    }
}
