/*
 * Copyright (c) 2025 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static io.tileverse.storage.StorageParameter.SUBGROUP_AUTHENTICATION;
import static java.util.function.Predicate.not;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.batch.BatchProviderHelper;
import io.tileverse.storage.batch.BatchSettings;
import io.tileverse.storage.s3.S3ClientCache.CredentialsSource;
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

    /**
     * The region of a store when nothing names one. Its endpoint routes a virtual-hosted request to the bucket by name,
     * in any region: unsigned reads of public buckets need no region. It is also the signing region expected by MinIO
     * and other S3-compatible services unless configured otherwise.
     */
    private static final String FALLBACK_REGION = "us-east-1";

    private final S3ClientCache clientCache;
    private final EnvironmentEndpoint environmentEndpoint;
    private final EnvironmentRegion environmentRegion;

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
     *
     * <p>Without a value, requests use path style when an endpoint override is in effect, and virtual-hosted style on
     * AWS. The parameter must not declare a default value: {@code StorageFactory} merges declared defaults into the
     * config before the provider reads it, and the default would replace that choice.
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

                Without a value, path style is used when a custom endpoint is in effect (storage.s3.endpoint, \
                the host of an http(s) URL of an S3-compatible service such as MinIO, or an endpoint named by \
                the AWS environment such as AWS_ENDPOINT_URL), and virtual-hosted style on AWS. Set this \
                parameter to override that choice.
                """)
            .type(Boolean.class)
            .group(ID)
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
     * <p>When set, this takes precedence over any endpoint inferred from an {@code http(s)://host/bucket/key} base URI,
     * and over an endpoint named by the AWS environment ({@code AWS_ENDPOINT_URL} and the like). Because custom
     * endpoints almost always require path-style addressing, a custom endpoint defaults {@link #S3_FORCE_PATH_STYLE} to
     * {@code true}; set {@code storage.s3.force-path-style} explicitly to override.
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
                    Without either, the store takes the endpoint named by the AWS environment: the 'aws.endpointUrlS3' \
                    or 'aws.endpointUrl' system property, the 'AWS_ENDPOINT_URL_S3' or 'AWS_ENDPOINT_URL' environment \
                    variable, then 'endpoint_url' in the AWS profile files. With none of them, the store uses the \
                    default AWS endpoints for its region.

                    Because custom endpoints almost always require path-style addressing, a custom endpoint from any \
                    of these sources defaults storage.s3.force-path-style to true. Set storage.s3.force-path-style \
                    explicitly to override.
                    """)
            .type(URI.class)
            .group(ID)
            .build();

    /** Configuration parameter for AWS S3 region. */
    public static final StorageParameter<String> S3_REGION = StorageParameter.builder()
            .key("storage.s3.region")
            .title("Region")
            .description("""
                    The AWS region of the bucket, for example us-west-2.

                    Without it, a store on AWS takes the region from the first of:

                    * the URL, when its host or bucket name holds a region;
                    * the 'aws.region' system property, then the 'AWS_REGION' environment variable;
                    * the AWS profile files: the credentials profile of the store, else the profile named by \
                    'AWS_PROFILE' or 'default';
                    * the EC2 metadata service, for a store on the AWS default credentials provider chain;
                    * us-east-1.

                    A request reaching the wrong region is sent again to the region of the bucket. Presigned URLs \
                    are not corrected: they need the right region.

                    A store with a service endpoint (see storage.s3.endpoint) ignores the region of the environment: \
                    its region only signs the requests, and is us-east-1 unless the URL or this parameter names one. \
                    Cloudflare R2 expects 'auto'.
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
        this(S3ClientCache.INSTANCE, EnvironmentEndpoint.INSTANCE, EnvironmentRegion.INSTANCE);
    }

    /**
     * Creates a provider leasing its clients from {@code clientCache}, and reading the endpoint and the region of the
     * environment from {@code environmentEndpoint} and {@code environmentRegion}. Tests inspect the cache after an
     * open, and pass an environment of their own.
     */
    S3StorageProvider(
            S3ClientCache clientCache, EnvironmentEndpoint environmentEndpoint, EnvironmentRegion environmentRegion) {
        super(true);
        this.clientCache = Objects.requireNonNull(clientCache, "clientCache");
        this.environmentEndpoint = Objects.requireNonNull(environmentEndpoint, "environmentEndpoint");
        this.environmentRegion = Objects.requireNonNull(environmentRegion, "environmentRegion");
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
        S3ClientCache.Lease lease = clientCache.acquire(keyFor(config, environmentEndpoint, environmentRegion));
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
     *   <li>without an endpoint override, the region found by {@code environmentRegion}
     *   <li>fallback to {@code us-east-1}
     * </ol>
     *
     * <p>Endpoint resolution order:
     *
     * <ol>
     *   <li>explicit {@code storage.s3.endpoint} in {@code StorageConfig}
     *   <li>endpoint parsed from an {@code http(s)://host/bucket/key} base URI
     *   <li>the endpoint found by {@code environmentEndpoint}
     *   <li>none (default AWS endpoints for the resolved region)
     * </ol>
     *
     * <p>{@code storage.s3.force-path-style} defaults to {@code true} whenever an endpoint override is in effect (from
     * any source) and {@code false} for canonical AWS URIs; an explicit value always wins.
     */
    static S3ClientCache.Key keyFor(
            StorageConfig config, EnvironmentEndpoint environmentEndpoint, EnvironmentRegion environmentRegion) {
        URI uri = config.baseUri();
        S3Reference s3Ref = S3CompatibleUrlParser.parseS3Url(uri);
        boolean anonymous = config.getParameter(S3_ANONYMOUS).orElse(false);
        Optional<String> accessKey = config.getParameter(S3_AWS_ACCESS_KEY_ID);
        Optional<String> secretKey = config.getParameter(S3_AWS_SECRET_ACCESS_KEY);
        Optional<String> profile = config.getParameter(S3_DEFAULT_CREDENTIALS_PROFILE);
        Optional<URI> endpointOverride = endpointOverride(config, s3Ref, profile, environmentEndpoint);
        boolean forcePathStyle = config.getParameter(S3_FORCE_PATH_STYLE).orElse(endpointOverride.isPresent());
        CredentialsSource credentials = CredentialsSource.of(anonymous, accessKey, secretKey, profile);
        Optional<String> named = namedRegion(config, s3Ref);
        String region = named.orElseGet(() -> unnamedRegion(endpointOverride, profile, credentials, environmentRegion));
        return new S3ClientCache.Key(
                region, endpointOverride, anonymous, accessKey, secretKey, profile, forcePathStyle);
    }

    /** The endpoint of the {@code storage.s3.endpoint} parameter, else of the URI, else of the environment. */
    private static Optional<URI> endpointOverride(
            StorageConfig config, S3Reference s3Ref, Optional<String> profile, EnvironmentEndpoint environment) {
        Optional<URI> parameter = config.getParameter(S3_ENDPOINT);
        return parameter.or(s3Ref::endpointOverride).or(() -> environment.find(profile));
    }

    /** The region of the {@code storage.s3.region} parameter, else the region parsed from the URI. */
    private static Optional<String> namedRegion(StorageConfig config, S3Reference s3Ref) {
        Optional<String> parameter = config.getParameter(S3_REGION).filter(not(String::isBlank));
        Optional<String> parsed = Optional.ofNullable(s3Ref.region()).filter(not(String::isBlank));
        return parameter.or(() -> parsed);
    }

    /**
     * The region of a store naming none. The environment names a region of AWS: a store on another endpoint ignores it.
     */
    private static String unnamedRegion(
            Optional<URI> endpointOverride,
            Optional<String> profile,
            CredentialsSource credentials,
            EnvironmentRegion environment) {
        if (endpointOverride.isPresent()) {
            return FALLBACK_REGION;
        }
        Optional<String> ofEnvironment = environment.find(profile, credentials);
        return ofEnvironment.orElse(FALLBACK_REGION);
    }
}
