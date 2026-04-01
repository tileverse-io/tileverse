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
package io.tileverse.geotools.parquet;

import io.tileverse.rangereader.azure.AzureBlobRangeReaderProvider;
import io.tileverse.rangereader.gcs.GoogleCloudStorageRangeReaderProvider;
import io.tileverse.rangereader.http.HttpRangeReaderProvider;
import io.tileverse.rangereader.s3.S3RangeReaderProvider;
import io.tileverse.rangereader.spi.AbstractRangeReaderProvider;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderParameter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.Parameter;
import org.geotools.util.Converters;

/**
 * Internal helper for the GeoParquet datastore factory to expose and translate RangeReader
 * parameters.
 *
 * <p>The GeoParquet datastore is backed by Tileverse {@code RangeReader}s, so it needs to expose
 * the same backend and authentication options understood by the available RangeReader providers.
 * This helper keeps that mapping local to the GeoParquet implementation:
 *
 * <ul>
 *   <li>it publishes provider-backed parameters as GeoTools {@link Param}s for the factory</li>
 *   <li>it translates the GeoTools connection map back to {@link Properties} for
 *   {@code RangeReaderFactory}</li>
 * </ul>
 *
 * <p>If another Tileverse GeoTools datastore ends up needing the same bridge, this class can be
 * extracted to a shared integration package without changing the GeoParquet factory contract.
 */
final class GeoParquetRangeReaderParams {

    static final Param RANGEREADER_PROVIDER_ID = dataStoreParam(RangeReaderConfig.FORCE_PROVIDER_ID);

    static final Param MEMORY_CACHE_ENABLED = dataStoreParam(AbstractRangeReaderProvider.MEMORY_CACHE_ENABLED);
    static final Param MEMORY_CACHE_BLOCK_ALIGNED =
            dataStoreParam(AbstractRangeReaderProvider.MEMORY_CACHE_BLOCK_ALIGNED);
    static final Param MEMORY_CACHE_BLOCK_SIZE = dataStoreParam(AbstractRangeReaderProvider.MEMORY_CACHE_BLOCK_SIZE);

    static final Param HTTP_CONNECTION_TIMEOUT_MILLIS =
            dataStoreParam(HttpRangeReaderProvider.HTTP_CONNECTION_TIMEOUT_MILLIS);
    static final Param HTTP_TRUST_ALL_SSL_CERTIFICATES =
            dataStoreParam(HttpRangeReaderProvider.HTTP_TRUST_ALL_SSL_CERTIFICATES);
    static final Param HTTP_AUTH_USERNAME = dataStoreParam(HttpRangeReaderProvider.HTTP_AUTH_USERNAME);
    static final Param HTTP_AUTH_PASSWORD = dataStoreParam(HttpRangeReaderProvider.HTTP_AUTH_PASSWORD);
    static final Param HTTP_AUTH_BEARER_TOKEN = dataStoreParam(HttpRangeReaderProvider.HTTP_AUTH_BEARER_TOKEN);
    static final Param HTTP_AUTH_API_KEY_HEADERNAME =
            dataStoreParam(HttpRangeReaderProvider.HTTP_AUTH_API_KEY_HEADERNAME);
    static final Param HTTP_AUTH_API_KEY = dataStoreParam(HttpRangeReaderProvider.HTTP_AUTH_API_KEY);
    static final Param HTTP_AUTH_API_KEY_VALUE_PREFIX =
            dataStoreParam(HttpRangeReaderProvider.HTTP_AUTH_API_KEY_VALUE_PREFIX);

    static final Param AZURE_BLOB_NAME = dataStoreParam(AzureBlobRangeReaderProvider.AZURE_BLOB_NAME);
    static final Param AZURE_ACCOUNT_KEY = dataStoreParam(AzureBlobRangeReaderProvider.AZURE_ACCOUNT_KEY);
    static final Param AZURE_SAS_TOKEN = dataStoreParam(AzureBlobRangeReaderProvider.AZURE_SAS_TOKEN);

    static final Param S3_FORCE_PATH_STYLE = dataStoreParam(S3RangeReaderProvider.S3_FORCE_PATH_STYLE);
    static final Param S3_REGION = dataStoreParam(S3RangeReaderProvider.S3_REGION);
    static final Param S3_AWS_ACCESS_KEY_ID = dataStoreParam(S3RangeReaderProvider.S3_AWS_ACCESS_KEY_ID);
    static final Param S3_AWS_SECRET_ACCESS_KEY = dataStoreParam(S3RangeReaderProvider.S3_AWS_SECRET_ACCESS_KEY);
    static final Param S3_USE_DEFAULT_CREDENTIALS_PROVIDER =
            dataStoreParam(S3RangeReaderProvider.S3_USE_DEFAULT_CREDENTIALS_PROVIDER);
    static final Param S3_DEFAULT_CREDENTIALS_PROFILE =
            dataStoreParam(S3RangeReaderProvider.S3_DEFAULT_CREDENTIALS_PROFILE);

    static final Param GCS_PROJECT_ID = dataStoreParam(GoogleCloudStorageRangeReaderProvider.GCS_PROJECT_ID);
    static final Param GCS_QUOTA_PROJECT_ID =
            dataStoreParam(GoogleCloudStorageRangeReaderProvider.GCS_QUOTA_PROJECT_ID);
    static final Param GCS_USE_DEFAULT_APPLICTION_CREDENTIALS =
            dataStoreParam(GoogleCloudStorageRangeReaderProvider.GCS_USE_DEFAULT_APPLICTION_CREDENTIALS);

    static final List<Param> PARAMETERS = List.of(
            RANGEREADER_PROVIDER_ID,
            MEMORY_CACHE_ENABLED,
            MEMORY_CACHE_BLOCK_ALIGNED,
            MEMORY_CACHE_BLOCK_SIZE,
            HTTP_CONNECTION_TIMEOUT_MILLIS,
            HTTP_TRUST_ALL_SSL_CERTIFICATES,
            HTTP_AUTH_USERNAME,
            HTTP_AUTH_PASSWORD,
            HTTP_AUTH_BEARER_TOKEN,
            HTTP_AUTH_API_KEY_HEADERNAME,
            HTTP_AUTH_API_KEY,
            HTTP_AUTH_API_KEY_VALUE_PREFIX,
            AZURE_BLOB_NAME,
            AZURE_ACCOUNT_KEY,
            AZURE_SAS_TOKEN,
            S3_FORCE_PATH_STYLE,
            S3_REGION,
            S3_AWS_ACCESS_KEY_ID,
            S3_AWS_SECRET_ACCESS_KEY,
            S3_USE_DEFAULT_CREDENTIALS_PROVIDER,
            S3_DEFAULT_CREDENTIALS_PROFILE,
            GCS_PROJECT_ID,
            GCS_QUOTA_PROJECT_ID,
            GCS_USE_DEFAULT_APPLICTION_CREDENTIALS);

    private GeoParquetRangeReaderParams() {}

    static Param[] getParameters() {
        return PARAMETERS.toArray(Param[]::new);
    }

    static Properties toProperties(Map<String, ?> connectionParams) {
        Properties config = new Properties();
        PARAMETERS.forEach(param -> addProperty(param, connectionParams, config));
        return config;
    }

    private static void addProperty(Param param, Map<String, ?> params, Properties config) {
        Object value;
        try {
            value = param.lookUp(params);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (value != null) {
            config.setProperty(param.key, Converters.convert(value, String.class));
        }
    }

    private static Param dataStoreParam(RangeReaderParameter<?> param) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(Parameter.LEVEL, param.group());
        if (param.password()) {
            metadata.put(Parameter.IS_PASSWORD, Boolean.TRUE);
        }
        if (!param.sampleValues().isEmpty()) {
            metadata.put(Parameter.OPTIONS, param.sampleValues().toArray());
        }

        return new Param(
                param.key(),
                param.type(),
                param.description(),
                false,
                param.defaultValue().orElse(null),
                metadata);
    }
}
