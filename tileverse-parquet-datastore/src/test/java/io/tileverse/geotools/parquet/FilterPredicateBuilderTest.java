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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Test;

class FilterPredicateBuilderTest {

    private static final FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
            """
            message test {
              required int32 id;
              required binary name (UTF8);
              required double value;
              required boolean active;
              required int64 timestamp;
            }
            """);

    private final FilterPredicateBuilder builder = new FilterPredicateBuilder();

    @Test
    void propertyIsEqualTo_int() {
        Filter filter = ff.equals(ff.property("id"), ff.literal(42));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.eq(FilterApi.intColumn("id"), 42);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void propertyIsGreaterThan() {
        Filter filter = ff.greater(ff.property("id"), ff.literal(10));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.gt(FilterApi.intColumn("id"), 10);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void propertyIsLessThanOrEqualTo() {
        Filter filter = ff.lessOrEqual(ff.property("value"), ff.literal(99.5));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.ltEq(FilterApi.doubleColumn("value"), 99.5);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void propertyIsNotEqualTo() {
        Filter filter = ff.notEqual(ff.property("id"), ff.literal(0));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.notEq(FilterApi.intColumn("id"), 0);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void stringEquality() {
        Filter filter = ff.equals(ff.property("name"), ff.literal("Alice"));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.eq(FilterApi.binaryColumn("name"), Binary.fromString("Alice"));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void andFilter() {
        Filter f1 = ff.equals(ff.property("id"), ff.literal(1));
        Filter f2 = ff.greater(ff.property("value"), ff.literal(5.0));
        Filter and = ff.and(f1, f2);

        FilterPredicate result = builder.convert(and, SCHEMA);
        FilterPredicate expected = FilterApi.and(
                FilterApi.eq(FilterApi.intColumn("id"), 1), FilterApi.gt(FilterApi.doubleColumn("value"), 5.0));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void orFilter() {
        Filter f1 = ff.equals(ff.property("id"), ff.literal(1));
        Filter f2 = ff.equals(ff.property("id"), ff.literal(2));
        Filter or = ff.or(f1, f2);

        FilterPredicate result = builder.convert(or, SCHEMA);
        FilterPredicate expected =
                FilterApi.or(FilterApi.eq(FilterApi.intColumn("id"), 1), FilterApi.eq(FilterApi.intColumn("id"), 2));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void notFilter() {
        Filter f = ff.equals(ff.property("active"), ff.literal(true));
        Filter not = ff.not(f);

        FilterPredicate result = builder.convert(not, SCHEMA);
        FilterPredicate expected = FilterApi.not(FilterApi.eq(FilterApi.booleanColumn("active"), true));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void propertyIsNull() {
        Filter filter = ff.isNull(ff.property("name"));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.eq(FilterApi.binaryColumn("name"), null);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void propertyIsBetween() {
        Filter filter = ff.between(ff.property("value"), ff.literal(10.0), ff.literal(20.0));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.and(
                FilterApi.gtEq(FilterApi.doubleColumn("value"), 10.0),
                FilterApi.ltEq(FilterApi.doubleColumn("value"), 20.0));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void swappedOperands_greaterWithLiteralFirst() {
        // literal(5) > property("id") should become property("id") < 5
        Filter filter = ff.greater(ff.literal(5), ff.property("id"));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.lt(FilterApi.intColumn("id"), 5);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void includeFilter_returnsNull() {
        FilterPredicate result = builder.convert(Filter.INCLUDE, SCHEMA);
        assertThat(result).isNull();
    }

    @Test
    void nullFilter_returnsNull() {
        FilterPredicate result = builder.convert(null, SCHEMA);
        assertThat(result).isNull();
    }

    @Test
    void spatialFilter_throwsUnsupported() {
        Filter bbox = ff.bbox("geom", -180, -90, 180, 90, "EPSG:4326");
        assertThatThrownBy(() -> builder.convert(bbox, SCHEMA)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void compoundAndWith3Children() {
        Filter f1 = ff.equals(ff.property("id"), ff.literal(1));
        Filter f2 = ff.greater(ff.property("value"), ff.literal(5.0));
        Filter f3 = ff.equals(ff.property("active"), ff.literal(true));
        Filter and = ff.and(java.util.List.of(f1, f2, f3));

        FilterPredicate result = builder.convert(and, SCHEMA);
        FilterPredicate expected = FilterApi.and(
                FilterApi.and(
                        FilterApi.eq(FilterApi.intColumn("id"), 1), FilterApi.gt(FilterApi.doubleColumn("value"), 5.0)),
                FilterApi.eq(FilterApi.booleanColumn("active"), true));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void propertyIsLessThan() {
        Filter filter = ff.less(ff.property("timestamp"), ff.literal(1000L));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.lt(FilterApi.longColumn("timestamp"), 1000L);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void propertyIsGreaterThanOrEqualTo() {
        Filter filter = ff.greaterOrEqual(ff.property("value"), ff.literal(0.0));
        FilterPredicate result = builder.convert(filter, SCHEMA);
        FilterPredicate expected = FilterApi.gtEq(FilterApi.doubleColumn("value"), 0.0);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void int96Column_equalityWithBigInteger() {
        MessageType int96Schema = Types.buildMessage()
                .addField(new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT96, "legacyTs"))
                .named("test");

        BigInteger value = BigInteger.valueOf(123456789L);
        Filter filter = ff.equals(ff.property("legacyTs"), ff.literal(value));
        FilterPredicate result = builder.convert(filter, int96Schema);

        // BigInteger should be converted to a 12-byte zero-padded Binary
        byte[] expected96 = new byte[12];
        byte[] raw = value.toByteArray();
        System.arraycopy(raw, 0, expected96, 12 - raw.length, raw.length);

        FilterPredicate expected =
                FilterApi.eq(FilterApi.binaryColumn("legacyTs"), Binary.fromReusedByteArray(expected96));
        assertThat(result).isEqualTo(expected);
    }
}
