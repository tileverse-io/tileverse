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
import java.net.URL;
import java.util.Map;
import java.util.logging.Logger;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.FileDataStore;
import org.geotools.api.data.FileDataStoreFactorySpi;
import org.geotools.util.logging.Logging;

public class GeoParquetFileDataStoreFactory implements FileDataStoreFactorySpi {

    private static final Logger LOGGER = Logging.getLogger(GeoParquetFileDataStoreFactory.class);

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
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Param[] getParametersInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isAvailable() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean canProcess(URL url) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public FileDataStore createDataStore(URL url) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getTypeName(URL url) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }
}
