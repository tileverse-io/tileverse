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
package io.tileverse.rangereader.gcs;

import static io.tileverse.rangereader.spi.RangeReaderParameter.SUBGROUP_AUTHENTICATION;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.spi.AbstractRangeReaderProvider;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderParameter;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link RangeReaderProvider} implementation for Google Cloud Storage.
 * <p>
 * The {@link RangeReaderConfig#uri() URI} is used to extract the bucket and object name from a GCS URI.
 * <p>
 * A GCS URL, or Google Cloud Storage Uniform Resource Locator, refers to the
 * address used to access resources stored within Google Cloud Storage. There
 * are several forms of GCS URLs, depending on the context and desired access
 * method:
 *
 * <ul>
 * <li>{@code gs://} URI: This is the canonical URI format for referencing
 * objects within Cloud Storage. It is commonly used within Google Cloud
 * services, tools, and libraries for internal referencing. For example:
 * <pre>
 * {@literal gs://your-bucket-name/your-object-name}
 * </pre>
 * <li>Public HTTP/HTTPS URLs: If an object is configured for public access, it
 * can be accessed directly via a standard HTTP or HTTPS URL. These URLs are
 * typically in the format:
 * <pre>
 * {@literal https://storage.googleapis.com/your-bucket-name/your-object-name}
 * </pre>
 * </ul>
 * When {@code http/s} URL schemes are used, {@link #canProcessHeaders(URI, Map)} disambiguates
 * by checking if a header starting with {@code x-goog-} was returned from the HEAD request.
 */
public class GoogleCloudStorageRangeReaderProvider extends AbstractRangeReaderProvider {

    /**
     * Key used as environment variable name to disable this range reader provider
     * <pre>
     * {@code export IO_TILEVERSE_RANGEREADER_GCS=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_RANGEREADER_GCS";
    /**
     * This range reader implementation's {@link #getId() unique identifier}
     */
    public static final String ID = "gcs";

    /**
     * Creates a new GoogleCloudStorageRangeReaderProvider with support for caching parameters
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_ENABLED
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_BLOCK_ALIGNED
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_BLOCK_SIZE
     */
    public GoogleCloudStorageRangeReaderProvider() {
        super(true);
    }

    /**
     * Project ID is a unique, user-defined identifier for a Google Cloud project.
     */
    public static final RangeReaderParameter<String> GCS_PROJECT_ID = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.gcs.project-id")
            .title("Google Cloud project ID")
            .description(
                    """
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

    /**
     * Quota ProjectId that specifies the project used for quota and billing purposes.
     */
    public static final RangeReaderParameter<String> GCS_QUOTA_PROJECT_ID = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.gcs.quota-project-id")
            .title("Quota Project ID")
            .description(
                    """
                    Quota ProjectId that specifies the project used for quota and billing purposes.

                    The caller must have serviceusage.services.use permission on the project.
                    """)
            .type(String.class)
            .group(ID)
            .build();

    /**
     * Use the default application credentials chain, defaults to {@code false}
     */
    public static final RangeReaderParameter<Boolean> GCS_USE_DEFAULT_APPLICTION_CREDENTIALS =
            RangeReaderParameter.builder()
                    .key("io.tileverse.rangereader.gcs.default-credentials-chain")
                    .title("Use the default application credentials chain")
                    .description(
                            """
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

    private static final List<RangeReaderParameter<?>> PARAMS =
            List.of(GCS_PROJECT_ID, GCS_QUOTA_PROJECT_ID, GCS_USE_DEFAULT_APPLICTION_CREDENTIALS);

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean isAvailable() {
        return RangeReaderProvider.isEnabled(ENABLED_KEY);
    }

    @Override
    public String getDescription() {
        return "Google Cloud Storage Range Reader";
    }

    @Override
    public int getOrder() {
        return -100; // High priority
    }

    @Override
    protected List<RangeReaderParameter<?>> buildParameters() {
        return PARAMS;
    }

    @Override
    public boolean canProcess(RangeReaderConfig config) {
        return RangeReaderConfig.matches(config, getId(), "gs", "http", "https");
    }

    @Override
    public boolean canProcessHeaders(URI uri, Map<String, List<String>> headers) {
        // Check for any "x-goog-" prefixed header, which is a strong signal for GCS.
        Set<String> headerNames = headers.keySet();
        boolean hasCustomHeaders =
                headerNames.stream().anyMatch(h -> h.toLowerCase().startsWith("x-goog-"));
        if (hasCustomHeaders) {
            return true;
        }
        String host = uri.getHost();
        return "storage.googleapis.com".equals(host) || "storage.cloud.google.com".equals(host);
    }

    /**
     *
     */
    @Override
    protected RangeReader createInternal(RangeReaderConfig opts) throws IOException {
        URI uri = opts.uri();
        return GoogleCloudStorageRangeReader.builder().uri(uri).build();
    }
}
