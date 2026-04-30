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
package io.tileverse.storage.gcs;

import static io.tileverse.storage.spi.StorageParameter.SUBGROUP_AUTHENTICATION;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.spi.AbstractStorageProvider;
import io.tileverse.storage.spi.StorageConfig;
import io.tileverse.storage.spi.StorageParameter;
import io.tileverse.storage.spi.StorageProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/**
 * {@link StorageProvider} implementation for Google Cloud Storage.
 *
 * <p>The {@link StorageConfig#uri() URI} is used to extract the bucket and object name from a GCS URI.
 *
 * <p>A GCS URL, or Google Cloud Storage Uniform Resource Locator, refers to the address used to access resources stored
 * within Google Cloud Storage. There are several forms of GCS URLs, depending on the context and desired access method:
 *
 * <ul>
 *   <li>{@code gs://} URI: This is the canonical URI format for referencing objects within Cloud Storage. It is
 *       commonly used within Google Cloud services, tools, and libraries for internal referencing. For example:
 *       <pre>
 * {@literal gs://your-bucket-name/your-object-name}
 * </pre>
 *   <li>Public HTTP/HTTPS URLs: If an object is configured for public access, it can be accessed directly via a
 *       standard HTTP or HTTPS URL. These URLs are typically in the format:
 *       <pre>
 * {@literal https://storage.googleapis.com/your-bucket-name/your-object-name}
 * </pre>
 * </ul>
 *
 * When {@code http/s} URL schemes are used, {@link #canProcessHeaders(URI, Map)} disambiguates by checking if a header
 * starting with {@code x-goog-} was returned from the HEAD request.
 */
public class GoogleCloudStorageProvider extends AbstractStorageProvider {

    /**
     * Key used as environment variable name to disable this range reader provider
     *
     * <pre>
     * {@code export IO_TILEVERSE_STORAGE_GCS=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_STORAGE_GCS";
    /** This range reader implementation's {@link #getId() unique identifier} */
    public static final String ID = "gcs";

    /**
     * Creates a new GoogleCloudStorageRangeReaderProvider with support for caching parameters
     *
     * @see AbstractStorageProvider#MEMORY_CACHE_ENABLED
     * @see AbstractStorageProvider#MEMORY_CACHE_BLOCK_ALIGNED
     * @see AbstractStorageProvider#MEMORY_CACHE_BLOCK_SIZE
     */
    public GoogleCloudStorageProvider() {
        super(true);
    }

    /**
     * Opens a {@link io.tileverse.storage.Storage} backed by the supplied GCS {@link com.google.cloud.storage.Storage}
     * client, bypassing the SPI configuration path. Useful for Spring-managed clients, fake-gcs-server fixtures, or
     * custom retry/transport configuration not expressible as scalar properties.
     *
     * <p>The returned {@code Storage} <b>borrows</b> the supplied client; closing the {@code Storage} does NOT close
     * the client. The caller retains ownership.
     *
     * @param uri canonical {@code gs://bucket/[prefix/]} URI; the bucket is parsed from this URI
     * @param client a pre-configured GCS Storage client; not closed by the returned {@code Storage}
     * @return a borrowed-client {@code GoogleCloudStorage}
     */
    public static Storage open(URI uri, com.google.cloud.storage.Storage client) {
        if (uri == null) {
            throw new IllegalArgumentException("uri");
        }
        if (client == null) {
            throw new IllegalArgumentException("client");
        }
        SdkStorageLocation location = SdkStorageLocation.parse(uri);
        return new GoogleCloudStorage(uri, location, new BorrowedGcsHandle(client));
    }

    /** Project ID is a unique, user-defined identifier for a Google Cloud project. */
    public static final StorageParameter<String> GCS_PROJECT_ID = StorageParameter.builder()
            .key("storage.gcs.project-id")
            .title("Google Cloud project ID")
            .description("""
                    Project ID is a unique, user-defined identifier for a Google Cloud project.

                    If no project ID is set, an attempt to obtain a default project ID from the \
                    environment will be made.

                    The default project ID will be obtained by the first available project ID \
                    among the following sources:
                    1. The project ID specified by the GOOGLE_CLOUD_PROJECT environment variable
                    2. The App Engine project ID
                    3. The project ID specified in the JSON credentials file pointed by the GOOGLE_APPLICATION_CREDENTIALS environment variable
                    4. The Google Cloud SDK project ID
                    5. The Compute Engine project ID
                    """)
            .type(String.class)
            .group(ID)
            .build();

    /** Quota ProjectId that specifies the project used for quota and billing purposes. */
    public static final StorageParameter<String> GCS_QUOTA_PROJECT_ID = StorageParameter.builder()
            .key("storage.gcs.quota-project-id")
            .title("Quota Project ID")
            .description("""
                    Quota ProjectId that specifies the project used for quota and billing purposes.

                    The caller must have serviceusage.services.use permission on the project.
                    """)
            .type(String.class)
            .group(ID)
            .build();

    /** Use the default application credentials chain, defaults to {@code false} */
    public static final StorageParameter<Boolean> GCS_USE_DEFAULT_APPLICTION_CREDENTIALS = StorageParameter.builder()
            .key("storage.gcs.default-credentials-chain")
            .title("Use the default application credentials chain")
            .description("""
                    Whether to use the default application credentials chain.

                    To set up Application Default Credentials for your environment, \
                    see https://cloud.google.com/docs/authentication/external/set-up-adc

                    Not doing so will lead to an error saying "Your default credentials were not found."
                    """)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .type(Boolean.class)
            .defaultValue(false)
            .build();

    /**
     * Custom GCS endpoint host override (e.g. {@code http://localhost:4443} for fake-gcs-server emulators). When set,
     * the provider talks to this host instead of the default {@code https://storage.googleapis.com}, and credentials
     * default to anonymous unless explicitly configured otherwise.
     *
     * <p><b>Key:</b> {@code storage.gcs.host}
     */
    public static final StorageParameter<String> GCS_HOST = StorageParameter.builder()
            .key("storage.gcs.host")
            .title("Custom GCS endpoint host")
            .description("""
                    Custom endpoint host for GCS-compatible servers (e.g. fake-gcs-server: http://localhost:4443). \
                    When set, the SDK targets this host instead of the public Google endpoint. \
                    Authentication defaults to anonymous when a host override is in effect.
                    """)
            .type(String.class)
            .group(ID)
            .build();

    private static final List<StorageParameter<?>> PARAMS =
            List.of(GCS_PROJECT_ID, GCS_QUOTA_PROJECT_ID, GCS_USE_DEFAULT_APPLICTION_CREDENTIALS, GCS_HOST);

    private final SdkStorageCache clientCache = new SdkStorageCache();

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
        return "Google Cloud Storage provider.";
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
        URI uri = config.uri();
        String scheme = uri.getScheme();
        if ("gs".equalsIgnoreCase(scheme)) {
            return StorageConfig.matches(config, getId(), "gs");
        }
        if (!StorageConfig.matches(config, getId(), "http", "https")) {
            return false;
        }
        return isRecognizedGcsHttpUri(uri);
    }

    @Override
    public boolean canProcessHeaders(URI uri, Map<String, List<String>> headers) {
        if (isRecognizedGcsHttpUri(uri)) {
            return true;
        }
        // Accept x-goog-* headers only for explicit emulator-style endpoints.
        Set<String> headerNames = headers.keySet();
        boolean hasGoogHeaders =
                headerNames.stream().anyMatch(h -> h.toLowerCase().startsWith("x-goog-"));
        return hasGoogHeaders && isEmulatorApiUri(uri);
    }

    private static boolean isRecognizedGcsHttpUri(URI uri) {
        String host = uri.getHost();
        if (host == null) {
            return false;
        }
        return "storage.googleapis.com".equals(host)
                || "storage.cloud.google.com".equals(host)
                || isEmulatorApiUri(uri);
    }

    private static boolean isEmulatorApiUri(URI uri) {
        String path = uri.getPath();
        return path != null && path.startsWith("/storage/v1/b/");
    }

    @Override
    public Storage createStorage(StorageConfig config) {
        URI uri = config.uri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.uri() is required for GoogleCloudStorageStorage");
        }
        SdkStorageLocation location = SdkStorageLocation.parse(uri);
        SdkStorageCache.Lease lease = clientCache.acquire(keyFor(config));
        return new GoogleCloudStorage(uri, location, lease);
    }

    /**
     * Resolves the {@link SdkStorageCache.Key} cache discriminator for the given config. Package-private so unit tests
     * can exercise host-override / project-id / credential precedence without acquiring a real SDK client lease.
     *
     * <p>Host-override resolution order:
     *
     * <ol>
     *   <li>explicit {@code storage.gcs.host} parameter (used for fake-gcs-server etc.)
     *   <li>derived from {@code http(s)://host/storage/v1/b/...} emulator-style URIs
     *   <li>none (use the default Google endpoint)
     * </ol>
     *
     * <p>When a host override is in effect, credentials default to anonymous unless the application default chain is
     * explicitly enabled.
     */
    static SdkStorageCache.Key keyFor(StorageConfig config) {
        URI uri = config.uri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.uri() is required for GoogleCloudStorageStorage");
        }
        Optional<String> projectId = config.getParameter(GCS_PROJECT_ID);
        Optional<String> hostOverride = config.getParameter(GCS_HOST).filter(s -> !s.isBlank());
        if (hostOverride.isEmpty()) {
            String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
            if ((scheme.equals("http") || scheme.equals("https"))
                    && uri.getPath() != null
                    && uri.getPath().startsWith("/storage/v1/b/")) {
                hostOverride = Optional.of(uri.getScheme() + "://" + uri.getAuthority());
            }
        }
        boolean useDefaultCreds =
                config.getParameter(GCS_USE_DEFAULT_APPLICTION_CREDENTIALS).orElse(true);
        boolean anonymous = !useDefaultCreds || hostOverride.isPresent();
        return new SdkStorageCache.Key(hostOverride, projectId, Optional.empty(), anonymous);
    }

    /**
     * Strip {@code ?alt=media} (or any other query string) from the leaf URI before the default split-into-(parent,
     * key) machinery runs. GCS REST-API URLs end in {@code ?alt=media} to fetch object content, but the query string is
     * not part of the object name; without this scrub the default impl would try to stat a blob whose name literally
     * contains the question mark.
     */
    @Override
    public RangeReader openRangeReader(StorageConfig leafConfig) {
        URI uri = leafConfig.uri();
        if (uri == null) {
            throw new IllegalArgumentException("StorageConfig.uri() is required");
        }
        if (uri.getRawQuery() == null && uri.getRawFragment() == null) {
            return openRangeReaderViaStorage(leafConfig);
        }
        URI scrubbed;
        try {
            scrubbed =
                    new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null /* query */, null /* fragment */);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Cannot strip query string from URI: " + uri, e);
        }
        Properties props = leafConfig.toProperties();
        props.setProperty(StorageConfig.URI_KEY, scrubbed.toString());
        return openRangeReaderViaStorage(StorageConfig.fromProperties(props));
    }
}
