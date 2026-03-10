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
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

class FilterPredicateBuilderCoverageTest {

    private static final FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
    private static final GeometryFactory gf = new GeometryFactory();

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
            """
            message test {
              required int32 id;
              required int32 d (DATE);
              required int64 ts (TIMESTAMP(MILLIS,true));
              required float ratio;
              required binary name (UTF8);
              required fixed_len_byte_array(4) code;
              required boolean active;
            }
            """);

    private final FilterPredicateBuilder builder = new FilterPredicateBuilder();

    @Test
    void dateAndTimestampLiterals_areConverted() {
        Filter dateFilter = ff.equals(ff.property("d"), ff.literal(LocalDate.of(2024, 1, 15)));
        Filter tsFilter = ff.equals(ff.property("ts"), ff.literal(Instant.ofEpochMilli(12345L)));

        FilterPredicate dateExpected = FilterApi.eq(
                FilterApi.intColumn("d"), (int) LocalDate.of(2024, 1, 15).toEpochDay());
        FilterPredicate tsExpected = FilterApi.eq(FilterApi.longColumn("ts"), 12345L);

        assertThat(builder.convert(dateFilter, SCHEMA)).isEqualTo(dateExpected);
        assertThat(builder.convert(tsFilter, SCHEMA)).isEqualTo(tsExpected);
    }

    @Test
    void fixedLengthBinary_acceptsByteArrayAndBinaryLiteral() {
        byte[] bytes = new byte[] {1, 2, 3, 4};
        Filter bytesFilter = ff.equals(ff.property("code"), ff.literal(bytes));
        Filter binaryFilter = ff.equals(ff.property("code"), ff.literal(Binary.fromReusedByteArray(bytes)));

        FilterPredicate expected = FilterApi.eq(FilterApi.binaryColumn("code"), Binary.fromReusedByteArray(bytes));
        assertThat(builder.convert(bytesFilter, SCHEMA)).isEqualTo(expected);
        assertThat(builder.convert(binaryFilter, SCHEMA)).isEqualTo(expected);
    }

    @Test
    void invalidComparisonOperands_throwUnsupported() {
        Filter bad = ff.equals(ff.property("id"), ff.property("d"));
        assertThatThrownBy(() -> builder.convert(bad, SCHEMA))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("requires a PropertyName and a Literal");
    }

    @Test
    void propertyIsBetweenAndNullWithInvalidExpression_throwUnsupported() {
        Filter betweenBad = ff.between(ff.literal(1), ff.literal(1), ff.literal(2));
        Filter isNullBad = ff.isNull(ff.literal("name"));

        assertThatThrownBy(() -> builder.convert(betweenBad, SCHEMA))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("PropertyIsBetween requires a PropertyName");
        assertThatThrownBy(() -> builder.convert(isNullBad, SCHEMA))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("PropertyIsNull requires a PropertyName");
    }

    @Test
    void includeNullAndLikeNilBranches_areCovered() {
        var visitor = new FilterPredicateBuilder.FilterToFilterPredicate(SCHEMA);
        assertThat(visitor.visitNullFilter(null)).isNull();
        assertThat(visitor.visit(Filter.INCLUDE, null)).isNull();

        assertThatThrownBy(() -> builder.convert(ff.like(ff.property("name"), "A%"), SCHEMA))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("PropertyIsLike");
        assertThatThrownBy(() -> builder.convert(ff.isNil(ff.property("name"), null), SCHEMA))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("PropertyIsNil");
    }

    @Test
    void booleanAndSwappedComparisons_areConverted() {
        Filter boolEq = ff.equals(ff.property("active"), ff.literal(true));
        Filter intGtSwapped = ff.greater(ff.literal(7), ff.property("id"));
        Filter intGeSwapped = ff.greaterOrEqual(ff.literal(7), ff.property("id"));
        Filter intLtSwapped = ff.less(ff.literal(7), ff.property("id"));
        Filter intLeSwapped = ff.lessOrEqual(ff.literal(7), ff.property("id"));

        assertThat(builder.convert(boolEq, SCHEMA)).isEqualTo(FilterApi.eq(FilterApi.booleanColumn("active"), true));
        assertThat(builder.convert(intGtSwapped, SCHEMA)).isEqualTo(FilterApi.lt(FilterApi.intColumn("id"), 7));
        assertThat(builder.convert(intGeSwapped, SCHEMA)).isEqualTo(FilterApi.ltEq(FilterApi.intColumn("id"), 7));
        assertThat(builder.convert(intLtSwapped, SCHEMA)).isEqualTo(FilterApi.gt(FilterApi.intColumn("id"), 7));
        assertThat(builder.convert(intLeSwapped, SCHEMA)).isEqualTo(FilterApi.gtEq(FilterApi.intColumn("id"), 7));
    }

    @Test
    void int96NegativeBigInteger_signExtendsTo12Bytes() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int96 legacyTs;
                }
                """);
        BigInteger negative = BigInteger.valueOf(-123456789L);
        Filter filter = ff.equals(ff.property("legacyTs"), ff.literal(negative));
        FilterPredicate actual = builder.convert(filter, schema);

        byte[] bytes = new byte[12];
        java.util.Arrays.fill(bytes, (byte) 0xFF);
        byte[] raw = negative.toByteArray();
        System.arraycopy(raw, 0, bytes, 12 - raw.length, raw.length);
        FilterPredicate expected = FilterApi.eq(FilterApi.binaryColumn("legacyTs"), Binary.fromReusedByteArray(bytes));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void int96ExactWidthBigInteger_keepsRawLength() {
        MessageType schema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required int96 legacyTs;
                }
                """);
        byte[] twelve = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        BigInteger exact12 = new BigInteger(1, twelve);
        Filter filter = ff.equals(ff.property("legacyTs"), ff.literal(exact12));
        FilterPredicate actual = builder.convert(filter, schema);
        FilterPredicate expected = FilterApi.eq(FilterApi.binaryColumn("legacyTs"), Binary.fromReusedByteArray(twelve));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void expressionVisitorUnsupportedOperations_areCovered() {
        var visitor = new FilterPredicateBuilder.FilterToFilterPredicate(SCHEMA);
        assertThat(ff.literal(42).accept(visitor, null)).isEqualTo(42);
        assertThat(ff.property("id").accept(visitor, null)).isEqualTo("id");
        assertThatThrownBy(() -> org.geotools.api.filter.expression.Expression.NIL.accept(visitor, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("NilExpression");

        assertThatThrownBy(() -> ff.add(ff.literal(1), ff.literal(2)).accept(visitor, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Add");
        assertThatThrownBy(() -> ff.divide(ff.literal(4), ff.literal(2)).accept(visitor, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Divide");
        assertThatThrownBy(() -> ff.multiply(ff.literal(2), ff.literal(3)).accept(visitor, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Multiply");
        assertThatThrownBy(() -> ff.subtract(ff.literal(4), ff.literal(1)).accept(visitor, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Subtract");
        assertThatThrownBy(() -> ff.function("abs", ff.literal(-2)).accept(visitor, null))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Function");
    }

    @Test
    void nullChecksAndNotEquals_coverPrimitiveBranches() {
        assertThat(builder.convert(ff.isNull(ff.property("id")), SCHEMA))
                .isEqualTo(FilterApi.eq(FilterApi.intColumn("id"), null));
        assertThat(builder.convert(ff.isNull(ff.property("ts")), SCHEMA))
                .isEqualTo(FilterApi.eq(FilterApi.longColumn("ts"), null));
        assertThat(builder.convert(ff.isNull(ff.property("ratio")), SCHEMA))
                .isEqualTo(FilterApi.eq(FilterApi.floatColumn("ratio"), null));
        assertThat(builder.convert(ff.isNull(ff.property("name")), SCHEMA))
                .isEqualTo(FilterApi.eq(FilterApi.binaryColumn("name"), null));
        assertThat(builder.convert(ff.isNull(ff.property("active")), SCHEMA))
                .isEqualTo(FilterApi.eq(FilterApi.booleanColumn("active"), null));

        assertThat(builder.convert(ff.notEqual(ff.property("id"), ff.literal(7)), SCHEMA))
                .isEqualTo(FilterApi.notEq(FilterApi.intColumn("id"), 7));
        assertThat(builder.convert(ff.notEqual(ff.property("ts"), ff.literal(8L)), SCHEMA))
                .isEqualTo(FilterApi.notEq(FilterApi.longColumn("ts"), 8L));
        assertThat(builder.convert(ff.notEqual(ff.property("ratio"), ff.literal(1.25f)), SCHEMA))
                .isEqualTo(FilterApi.notEq(FilterApi.floatColumn("ratio"), 1.25f));
        assertThat(builder.convert(ff.notEqual(ff.property("name"), ff.literal("x")), SCHEMA))
                .isEqualTo(FilterApi.notEq(FilterApi.binaryColumn("name"), Binary.fromString("x")));
        assertThat(builder.convert(ff.notEqual(ff.property("active"), ff.literal(true)), SCHEMA))
                .isEqualTo(FilterApi.notEq(FilterApi.booleanColumn("active"), true));
    }

    @Test
    void orderedComparisons_coverFloatAndBinaryBranches() {
        assertThat(builder.convert(ff.greater(ff.property("ratio"), ff.literal(1.1f)), SCHEMA))
                .isEqualTo(FilterApi.gt(FilterApi.floatColumn("ratio"), 1.1f));
        assertThat(builder.convert(ff.greaterOrEqual(ff.property("ratio"), ff.literal(1.1f)), SCHEMA))
                .isEqualTo(FilterApi.gtEq(FilterApi.floatColumn("ratio"), 1.1f));
        assertThat(builder.convert(ff.less(ff.property("ratio"), ff.literal(1.1f)), SCHEMA))
                .isEqualTo(FilterApi.lt(FilterApi.floatColumn("ratio"), 1.1f));
        assertThat(builder.convert(ff.lessOrEqual(ff.property("ratio"), ff.literal(1.1f)), SCHEMA))
                .isEqualTo(FilterApi.ltEq(FilterApi.floatColumn("ratio"), 1.1f));

        assertThat(builder.convert(ff.greater(ff.property("name"), ff.literal("aa")), SCHEMA))
                .isEqualTo(FilterApi.gt(FilterApi.binaryColumn("name"), Binary.fromString("aa")));
        assertThat(builder.convert(ff.less(ff.property("name"), ff.literal("zz")), SCHEMA))
                .isEqualTo(FilterApi.lt(FilterApi.binaryColumn("name"), Binary.fromString("zz")));
    }

    @Test
    void orderedComparisons_coverRemainingGtEqLtEqBranches() {
        assertThat(builder.convert(ff.greaterOrEqual(ff.property("id"), ff.literal(10)), SCHEMA))
                .isEqualTo(FilterApi.gtEq(FilterApi.intColumn("id"), 10));
        assertThat(builder.convert(ff.greaterOrEqual(ff.property("ts"), ff.literal(20L)), SCHEMA))
                .isEqualTo(FilterApi.gtEq(FilterApi.longColumn("ts"), 20L));
        assertThat(builder.convert(ff.greaterOrEqual(ff.property("name"), ff.literal("mm")), SCHEMA))
                .isEqualTo(FilterApi.gtEq(FilterApi.binaryColumn("name"), Binary.fromString("mm")));

        assertThat(builder.convert(ff.lessOrEqual(ff.property("id"), ff.literal(10)), SCHEMA))
                .isEqualTo(FilterApi.ltEq(FilterApi.intColumn("id"), 10));
        assertThat(builder.convert(ff.lessOrEqual(ff.property("ts"), ff.literal(20L)), SCHEMA))
                .isEqualTo(FilterApi.ltEq(FilterApi.longColumn("ts"), 20L));
        assertThat(builder.convert(ff.lessOrEqual(ff.property("name"), ff.literal("mm")), SCHEMA))
                .isEqualTo(FilterApi.ltEq(FilterApi.binaryColumn("name"), Binary.fromString("mm")));
    }

    @Test
    void equalityBranches_coverLongFloatDoubleAndNullDouble() {
        assertThat(builder.convert(ff.equals(ff.property("ts"), ff.literal(77L)), SCHEMA))
                .isEqualTo(FilterApi.eq(FilterApi.longColumn("ts"), 77L));
        assertThat(builder.convert(ff.equals(ff.property("ratio"), ff.literal(3.5f)), SCHEMA))
                .isEqualTo(FilterApi.eq(FilterApi.floatColumn("ratio"), 3.5f));
        assertThat(builder.convert(ff.equals(ff.property("ratio"), ff.literal(3.5d)), SCHEMA))
                .isEqualTo(FilterApi.eq(FilterApi.floatColumn("ratio"), 3.5f));

        MessageType numericSchema = MessageTypeParser.parseMessageType(
                """
                message test {
                  required double amount;
                }
                """);
        assertThat(builder.convert(ff.equals(ff.property("amount"), ff.literal(9.25d)), numericSchema))
                .isEqualTo(FilterApi.eq(FilterApi.doubleColumn("amount"), 9.25d));
        assertThat(builder.convert(ff.notEqual(ff.property("amount"), ff.literal(1.25d)), numericSchema))
                .isEqualTo(FilterApi.notEq(FilterApi.doubleColumn("amount"), 1.25d));
        assertThat(builder.convert(ff.isNull(ff.property("amount")), numericSchema))
                .isEqualTo(FilterApi.eq(FilterApi.doubleColumn("amount"), null));
    }

    @Test
    void unsupportedSpatialTemporalAndIdFilters_areCovered() {
        Point point = gf.createPoint(new org.locationtech.jts.geom.Coordinate(0, 0));
        List<Filter> filters = List.of(
                Filter.EXCLUDE,
                ff.id(Set.of(ff.featureId("fid-1"))),
                ff.bbox("geom", -10, -10, 10, 10, "EPSG:4326"),
                ff.beyond("geom", point, 1.0, "meters"),
                ff.contains("geom", point),
                ff.crosses("geom", point),
                ff.disjoint("geom", point),
                ff.dwithin("geom", point, 1.0, "meters"),
                ff.equals("geom", point),
                ff.intersects("geom", point),
                ff.overlaps("geom", point),
                ff.touches("geom", point),
                ff.within("geom", point),
                ff.after(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.anyInteracts(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.before(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.begins(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.begunBy(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.during(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.endedBy(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.ends(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.meets(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.metBy(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.overlappedBy(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.tcontains(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.tequals(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))),
                ff.toverlaps(ff.property("ts"), ff.literal(Instant.ofEpochMilli(1))));

        for (Filter filter : filters) {
            assertThatThrownBy(() -> builder.convert(filter, SCHEMA)).isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
