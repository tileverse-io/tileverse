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
package io.tileverse.rangereader.azure;

import static io.tileverse.rangereader.spi.RangeReaderParameter.SUBGROUP_AUTHENTICATION;

import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.StorageSharedKeyCredential;
import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.azure.AzureBlobRangeReader.Builder;
import io.tileverse.rangereader.spi.AbstractRangeReaderProvider;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderParameter;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * {@link RangeReaderProvider} implementation for Azure Blob Storage.
 * <p>
 * The {@link RangeReaderConfig#uri() URI} is used to extract the account, container, and blob name from an Azure Blob Storage URI.
 * <p>
 * An Azure Blob Storage URL, or Uniform Resource Locator, refers to the
 * address used to access resources stored within Azure Blob Storage. There
 * are several forms of Azure Blob Storage URLs, depending on the context and desired access
 * method:
 *
 * <ul>
 * <li>{@code https://} URI: This is the canonical URI format for referencing
 * objects within Azure Blob Storage. It is commonly used within Azure
 * services, tools, and libraries for internal referencing. For example:
 * <pre>
 * {@literal https://your-account-name.blob.core.windows.net/your-container-name/your-blob-name}
 * </pre>
 * </ul>
 * When {@code http/s} URL schemes are used, {@link #canProcessHeaders(URI, Map)} disambiguates
 * by checking if a header starting with {@code x-ms-} was returned from the HEAD request.
 */
public class AzureBlobRangeReaderProvider extends AbstractRangeReaderProvider {

    /**
     * Key used as environment variable name to disable this range reader provider
     * <pre>
     * {@code export IO_TILEVERSE_RANGEREADER_AZURE=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_RANGEREADER_AZURE";
    /**
     * This range reader implementation's {@link #getId() unique identifier}
     */
    public static final String ID = "azure";

    /**
     * Creates a new AzureBlobRangeReaderProvider with support for caching parameters
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_ENABLED
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_BLOCK_ALIGNED
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_BLOCK_SIZE
     */
    public AzureBlobRangeReaderProvider() {
        super(true);
    }

    /**
     * Set the blob name if the endpoint points to the account url
     *
     * @see BlobClientBuilder#blobName(String)
     */
    public static final RangeReaderParameter<String> AZURE_BLOB_NAME = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.azure.blob-name")
            .title("Set the blob name if the endpoint points to the account url")
            .description(
                    """
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
     * The account access key used to authenticate the request.
     * @see StorageSharedKeyCredential
     */
    public static final RangeReaderParameter<String> AZURE_ACCOUNT_KEY = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.azure.account-key")
            .title("Account access key")
            .description(
                    """
                    The account access key used to authenticate the request.

                    When specified, the account name obtained from the URI will be used with this \
                    access key to create a SharedKey credential policy that is put into a header \
                    to authorize requests
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /**
     * SAS token to use for authenticating requests
     */
    public static final RangeReaderParameter<String> AZURE_SAS_TOKEN = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.azure.sas-token")
            .title("SAS token to use for authenticating requests")
            .description(
                    """
                    Shared Access Signature, a security token generated on the client side to grant limited, \
                    delegated access to Azure resources.

                    This token can also be in the blob URL query string.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    private static final List<RangeReaderParameter<?>> PARAMS =
            List.of(AZURE_BLOB_NAME, AZURE_ACCOUNT_KEY, AZURE_SAS_TOKEN);

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
        return "Azure Blob Storage Range Reader";
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
        if (!RangeReaderConfig.matches(config, getId(), "http", "https")) {
            return false;
        }
        URI endpointUrl = config.uri();
        BlobUrlParts parts;
        try {
            parts = BlobUrlParts.parse(endpointUrl.toString());
        } catch (Exception e) {
            return false;
        }
        if (parts.getHost() == null || parts.getBlobContainerName() == null) {
            return false;
        }
        String blobName = parts.getBlobName();
        if (blobName == null) {
            return config.getParameter(AZURE_BLOB_NAME).isPresent();
        }
        return true;
    }

    @Override
    public boolean canProcessHeaders(URI uri, Map<String, List<String>> headers) {
        return headers.containsKey("x-ms-request-id");
    }

    @Override
    protected RangeReader createInternal(RangeReaderConfig opts) throws IOException {
        URI uri = opts.uri();
        Builder builder = AzureBlobRangeReader.builder().endpoint(uri);
        opts.getParameter(AZURE_BLOB_NAME).ifPresent(builder::blobName);
        opts.getParameter(AZURE_ACCOUNT_KEY).ifPresent(builder::accountKey);
        opts.getParameter(AZURE_SAS_TOKEN).ifPresent(builder::sasToken);
        return builder.build();
    }
}
