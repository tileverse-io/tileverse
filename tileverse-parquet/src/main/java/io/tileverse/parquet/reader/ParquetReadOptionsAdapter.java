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
package io.tileverse.parquet.reader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.parquet.filter2.compat.FilterCompat;

public final class ParquetReadOptionsAdapter {
    private ParquetReadOptionsAdapter() {}

    public static CoreParquetReadOptions fromObject(Object options) {
        if (options == null) {
            return CoreParquetReadOptions.defaults();
        }

        CoreParquetReadOptions.Builder builder = CoreParquetReadOptions.builder();
        Boolean useStats = callBoolean(options, "useStatsFilter");
        if (useStats != null) {
            builder.useStatsFilter(useStats);
        }

        Boolean useDictionary = callBoolean(options, "useDictionaryFilter");
        if (useDictionary != null) {
            builder.useDictionaryFilter(useDictionary);
        }

        Boolean useColumnIndex = callBoolean(options, "useColumnIndexFilter");
        if (useColumnIndex != null) {
            builder.useColumnIndexFilter(useColumnIndex);
        }

        Object recordFilter = call(options, "getRecordFilter");
        if (recordFilter instanceof FilterCompat.Filter filter) {
            builder.withRecordFilter(filter);
        }

        return builder.build();
    }

    private static Object call(Object target, String methodName) {
        try {
            Method method = target.getClass().getMethod(methodName);
            return method.invoke(target);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return null;
        }
    }

    private static Boolean callBoolean(Object target, String methodName) {
        Object result = call(target, methodName);
        return result instanceof Boolean b ? b : null;
    }
}
