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
package io.tileverse.storage.azure;

import static io.tileverse.storage.StorageParameter.SUBGROUP_AUTHENTICATION;

import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.StorageSharedKeyCredential;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageParameter;
import io.tileverse.storage.spi.AbstractStorageProvider;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * {@link StorageProvider} implementation for Azure Blob Storage.
 *
 * <p>The {@link StorageConfig#baseUri() URI} is used to extract the account, container, and blob name from an Azure
 * Blob Storage URI.
 *
 * <p>An Azure Blob Storage URL, or Uniform Resource Locator, refers to the address used to access resources stored
 * within Azure Blob Storage. There are several forms of Azure Blob Storage URLs, depending on the context and desired
 * access method:
 *
 * <ul>
 *   <li>{@code https://} URI: This is the canonical URI format for referencing objects within Azure Blob Storage. It is
 *       commonly used within Azure services, tools, and libraries for internal referencing. For example:
 *       <pre>
 * {@literal https://your-account-name.blob.core.windows.net/your-container-name/your-blob-name}
 * </pre>
 * </ul>
 *
 * When {@code http/s} URL schemes are used, {@link #canProcessHeaders(URI, Map)} disambiguates by checking if a header
 * starting with {@code x-ms-} was returned from the HEAD request.
 */
public class AzureBlobStorageProvider extends AbstractStorageProvider {

    private final AzureClientCache clientCache = new AzureClientCache();

    /**
     * Key used as environment variable name to disable this range reader provider
     *
     * <pre>
     * {@code export IO_TILEVERSE_STORAGE_AZURE=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_STORAGE_AZURE";

    /** This range reader implementation's {@link #getId() unique identifier} */
    public static final String ID = "azure";

    /**
     * Set the blob name if the endpoint points to the account url
     *
     * @see BlobClientBuilder#blobName(String)
     */
    public static final StorageParameter<String> AZURE_BLOB_NAME = StorageParameter.builder()
            .key("storage.azure.blob-name")
            .title("Set the blob name if the endpoint points to the account url")
            .description("""
                    Sets the blob path (e.g. /path/to/file.pmtiles).

                    If the endpoint URL is to a blob in the root container, parsing will fail as it will interpret the blob name \
                    as the container name. With only one path element, it is impossible to distinguish between a container name and \
                    a blob in the root container, so it is assumed to be the container name as this is much more common

                    When working with blobs in the root container, it is best to set the endpoint to the account url and specify the \
                    blob name separately using this parameter.
                    """)
            .type(String.class)
            .group(ID)
            .build();

    /**
     * When {@code true}, build the Azure SDK client with no credential at all. Use this for containers configured with
     * Container- or Blob-level anonymous access (e.g. open public datasets). Takes precedence over
     * {@link #AZURE_CONNECTION_STRING}, {@link #AZURE_ACCOUNT_KEY}, {@link #AZURE_SAS_TOKEN}, and the default
     * credential chain.
     */
    public static final StorageParameter<Boolean> AZURE_ANONYMOUS = StorageParameter.builder()
            .key("storage.azure.anonymous")
            .title("Anonymous public access")
            .description("""
                    When true, the Azure SDK client is built without any credential. \
                    Use for public containers configured with Container- or Blob-level anonymous access. \
                    Takes precedence over connection-string, account-key, sas-token, and the default credential chain.
                    """)
            .type(Boolean.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .defaultValue(false)
            .build();

    /**
     * The account access key used to authenticate the request.
     *
     * @see StorageSharedKeyCredential
     */
    public static final StorageParameter<String> AZURE_ACCOUNT_KEY = StorageParameter.builder()
            .key("storage.azure.account-key")
            .title("Account access key")
            .description("""
                    The account access key used to authenticate the request.

                    When specified, the account name obtained from the URI will be used with this \
                    access key to create a SharedKey credential policy that is put into a header \
                    to authorize requests
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /** SAS token to use for authenticating requests */
    public static final StorageParameter<String> AZURE_SAS_TOKEN = StorageParameter.builder()
            .key("storage.azure.sas-token")
            .title("SAS token to use for authenticating requests")
            .description("""
                    Shared Access Signature, a security token generated on the client side to grant limited, \
                    delegated access to Azure resources.

                    This token can also be in the blob URL query string.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /**
     * Azure Storage connection string. When set, takes precedence over account key / SAS token / default credential.
     * Useful for development against Azurite.
     *
     * <p><b>Key:</b> {@code storage.azure.connection-string}
     */
    public static final StorageParameter<String> AZURE_CONNECTION_STRING = StorageParameter.builder()
            .key("storage.azure.connection-string")
            .title("Azure Storage connection string")
            .description("""
                    Full connection string (DefaultEndpointsProtocol=...;AccountName=...;AccountKey=...;EndpointSuffix=...). \
                    When set, takes precedence over account-key, sas-token, and the default credential chain. \
                    Common for Azurite and local development.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .password(true)
            .build();

    /** Maximum number of retry attempts for Azure SDK requests. Default {@code 3}. */
    public static final StorageParameter<Integer> AZURE_MAX_RETRIES = StorageParameter.builder()
            .key("storage.azure.max-retries")
            .title("Max retry attempts")
            .description("Maximum number of retry attempts for failed Azure SDK requests. Default 3.")
            .type(Integer.class)
            .group(ID)
            .defaultValue(3)
            .build();

    /** Initial retry delay (exponential backoff). Default {@code PT4S}. */
    public static final StorageParameter<Duration> AZURE_RETRY_DELAY = StorageParameter.builder()
            .key("storage.azure.retry-delay")
            .title("Retry initial delay")
            .description(
                    "Initial delay before the first retry; doubles on each subsequent retry up to max-retry-delay. ISO-8601 duration. Default PT4S.")
            .type(Duration.class)
            .group(ID)
            .defaultValue(Duration.ofSeconds(4))
            .build();

    /** Cap on retry backoff delay. Default {@code PT2M}. */
    public static final StorageParameter<Duration> AZURE_MAX_RETRY_DELAY = StorageParameter.builder()
            .key("storage.azure.max-retry-delay")
            .title("Max retry delay")
            .description("Upper bound on retry backoff delay. ISO-8601 duration. Default PT2M (120s).")
            .type(Duration.class)
            .group(ID)
            .defaultValue(Duration.ofMinutes(2))
            .build();

    /** Per-request timeout. Default {@code PT60S}. */
    public static final StorageParameter<Duration> AZURE_TRY_TIMEOUT = StorageParameter.builder()
            .key("storage.azure.try-timeout")
            .title("Per-attempt timeout")
            .description("Timeout for each individual request attempt. ISO-8601 duration. Default PT60S.")
            .type(Duration.class)
            .group(ID)
            .defaultValue(Duration.ofSeconds(60))
            .build();

    private static final List<StorageParameter<?>> PARAMS = List.of(
            AZURE_BLOB_NAME,
            AZURE_ANONYMOUS,
            AZURE_ACCOUNT_KEY,
            AZURE_SAS_TOKEN,
            AZURE_CONNECTION_STRING,
            AZURE_MAX_RETRIES,
            AZURE_RETRY_DELAY,
            AZURE_MAX_RETRY_DELAY,
            AZURE_TRY_TIMEOUT);

    /**
     * Creates a new AzureBlobRangeReaderProvider with support for caching parameters
     *
     * @see AbstractStorageProvider#MEMORY_CACHE_ENABLED
     * @see AbstractStorageProvider#MEMORY_CACHE_BLOCK_ALIGNED
     * @see AbstractStorageProvider#MEMORY_CACHE_BLOCK_SIZE
     */
    public AzureBlobStorageProvider() {
        super(true);
    }

    /**
     * Opens a {@link Storage} backed by the supplied {@link BlobServiceClient}, bypassing the SPI configuration path.
     * Useful for Spring-managed clients, Azurite fixtures, or custom retry-policy configurations expressed via the
     * typed {@code RequestRetryOptions} (which th e SPI parameter set does not cover).
     *
     * <p>The returned {@code Storage} <b>borrows</b> the supplied client; closing the {@code Storage} does NOT close
     * the client. The caller retains ownership.
     *
     * @param uri canonical Azure Blob URI down to the container (e.g.
     *     {@code https://account.blob.core.windows.net/container/[prefix/]})
     * @param client a pre-configured Blob service client; not closed by the returned {@code Storage}
     * @return a borrowed-client {@code AzureBlobStorage}
     */
    public static Storage open(URI uri, BlobServiceClient client) {
        if (uri == null) {
            throw new IllegalArgumentException("uri");
        }
        if (client == null) {
            throw new IllegalArgumentException("client");
        }
        AzureBlobLocation location = AzureBlobLocation.parse(uri);
        return new AzureBlobStorage(uri, location, new BorrowedAzureHandle(client));
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
        return "Azure Blob Storage provider.";
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
        if (!matches(config, "https", "http", "az")) {
            return false;
        }
        URI uri = config.baseUri();
        // az:// is parsed by AzureBlobLocation directly. http(s) URIs go through BlobUrlParts so we keep
        // the existing host-shape validation (account name embedded in host, container in path).
        if ("az".equalsIgnoreCase(uri.getScheme())) {
            try {
                AzureBlobLocation loc = AzureBlobLocation.parse(uri);
                return loc.container() != null && !loc.container().isEmpty();
            } catch (RuntimeException e) {
                return false;
            }
        }
        BlobUrlParts parts;
        try {
            parts = parseBlobUrlParts(uri);
        } catch (RuntimeException e) {
            return false;
        }
        // Both blob-rooted URIs and container-rooted URIs are valid Storage roots: openRangeReader(key)
        // supplies the leaf at read time.
        return parts.getHost() != null
                && parts.getBlobContainerName() != null
                && !parts.getBlobContainerName().isEmpty();
    }

    static BlobUrlParts parseBlobUrlParts(URI endpointUrl) {
        String scheme = endpointUrl.getScheme();
        if (!("https".equals(scheme) || "http".equals(scheme))) {
            throw new IllegalArgumentException("URI must have https, or blob scheme: " + endpointUrl);
        }
        String url = endpointUrl.toString();
        return BlobUrlParts.parse(url);
    }

    @Override
    public boolean canProcessHeaders(URI uri, Map<String, List<String>> headers) {
        return headers.containsKey("x-ms-request-id");
    }

    @Override
    public Storage createStorage(StorageConfig config) {
        URI uri = config.baseUri();
        AzureBlobLocation location = AzureBlobLocation.parse(uri);
        AzureClientCache.Lease lease = clientCache.acquire(keyFor(config, location));
        return new AzureBlobStorage(uri, location, lease);
    }

    /**
     * Resolves the {@link AzureClientCache.Key} cache discriminator for the given config and parsed location.
     * Package-private so unit tests can exercise credential / connection-string / retry-tuning precedence without
     * acquiring a real SDK client lease.
     */
    static AzureClientCache.Key keyFor(StorageConfig config, AzureBlobLocation location) {
        boolean anonymous = config.getParameter(AZURE_ANONYMOUS).orElse(Boolean.FALSE);
        return new AzureClientCache.Key(
                location.endpoint(),
                location.accountName(),
                config.getParameter(AZURE_ACCOUNT_KEY),
                config.getParameter(AZURE_SAS_TOKEN),
                config.getParameter(AZURE_CONNECTION_STRING),
                anonymous,
                retryFromConfig(config));
    }

    static AzureRetryConfig retryFromConfig(StorageConfig config) {
        return new AzureRetryConfig(
                config.getParameter(AZURE_MAX_RETRIES)
                        .orElseGet(() -> AZURE_MAX_RETRIES.defaultValue().orElseThrow()),
                config.getParameter(AZURE_RETRY_DELAY)
                        .orElseGet(() -> AZURE_RETRY_DELAY.defaultValue().orElseThrow()),
                config.getParameter(AZURE_MAX_RETRY_DELAY)
                        .orElseGet(() -> AZURE_MAX_RETRY_DELAY.defaultValue().orElseThrow()),
                config.getParameter(AZURE_TRY_TIMEOUT)
                        .orElseGet(() -> AZURE_TRY_TIMEOUT.defaultValue().orElseThrow()));
    }
}
