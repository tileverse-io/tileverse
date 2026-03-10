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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.FileDataStore;
import org.geotools.api.data.FileDataStoreFactorySpi;
import org.geotools.util.logging.Logging;

public class GeoParquetFileDataStoreFactory implements FileDataStoreFactorySpi {

    private static final Logger LOGGER = Logging.getLogger(GeoParquetFileDataStoreFactory.class);

    static final String URL_PARAM = "url";
    static final Param URLP = new Param(URL_PARAM, URL.class, "GeoParquet URL to read", true, null);

    @Override
    public String[] getFileExtensions() {
        return new String[] {".parquet"};
    }

    @Override
    public String getDisplayName() {
        return "GeoParquet";
    }

    @Override
    public String getDescription() {
        return "GeoParquet pure java";
    }

    @Override
    public DataStore createDataStore(Map<String, ?> params) throws IOException {
        URL url = toUrl(params.get(URL_PARAM));
        return createDataStore(url);
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        throw new IOException("GeoParquet datastore is read-only; createNewDataStore is not supported");
    }

    @Override
    public Param[] getParametersInfo() {
        return new Param[] {URLP};
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public boolean canProcess(URL url) {
        if (url == null) {
            return false;
        }
        String path = url.getPath();
        if (path == null || path.isEmpty()) {
            return false;
        }
        String lowerPath = path.toLowerCase(Locale.ROOT);
        return lowerPath.endsWith(".parquet");
    }

    @Override
    public FileDataStore createDataStore(URL url) throws IOException {
        if (!canProcess(url)) {
            throw new IOException("Unsupported URL for GeoParquet datastore: " + url);
        }
        LOGGER.fine(() -> "GeoParquet FileDataStore requested for URL: " + url);
        return GeoParquetFileDataStore.open(url);
    }

    @Override
    public String getTypeName(URL url) throws IOException {
        if (!canProcess(url)) {
            throw new IOException("Unsupported URL for GeoParquet datastore: " + url);
        }
        return GeoParquetFileDataStore.typeNameFrom(url);
    }

    private static URL toUrl(Object value) throws IOException {
        if (value instanceof URL url) {
            return url;
        }
        if (value == null) {
            throw new IOException("Missing required parameter '" + URL_PARAM + "'");
        }
        try {
            return new URL(value.toString());
        } catch (MalformedURLException e) {
            throw new IOException("Invalid '" + URL_PARAM + "' parameter value: " + value, e);
        }
    }
}
