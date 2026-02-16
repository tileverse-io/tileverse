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

import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.BooleanColumn;
import org.apache.parquet.filter2.predicate.Operators.Column;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.geotools.api.filter.And;
import org.geotools.api.filter.ExcludeFilter;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterVisitor;
import org.geotools.api.filter.Id;
import org.geotools.api.filter.IncludeFilter;
import org.geotools.api.filter.Not;
import org.geotools.api.filter.Or;
import org.geotools.api.filter.PropertyIsBetween;
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.PropertyIsGreaterThan;
import org.geotools.api.filter.PropertyIsGreaterThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLessThan;
import org.geotools.api.filter.PropertyIsLessThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLike;
import org.geotools.api.filter.PropertyIsNil;
import org.geotools.api.filter.PropertyIsNotEqualTo;
import org.geotools.api.filter.PropertyIsNull;
import org.geotools.api.filter.expression.Add;
import org.geotools.api.filter.expression.Divide;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.ExpressionVisitor;
import org.geotools.api.filter.expression.Function;
import org.geotools.api.filter.expression.Literal;
import org.geotools.api.filter.expression.Multiply;
import org.geotools.api.filter.expression.NilExpression;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.api.filter.expression.Subtract;
import org.geotools.api.filter.spatial.BBOX;
import org.geotools.api.filter.spatial.Beyond;
import org.geotools.api.filter.spatial.Contains;
import org.geotools.api.filter.spatial.Crosses;
import org.geotools.api.filter.spatial.DWithin;
import org.geotools.api.filter.spatial.Disjoint;
import org.geotools.api.filter.spatial.Equals;
import org.geotools.api.filter.spatial.Intersects;
import org.geotools.api.filter.spatial.Overlaps;
import org.geotools.api.filter.spatial.Touches;
import org.geotools.api.filter.spatial.Within;
import org.geotools.api.filter.temporal.After;
import org.geotools.api.filter.temporal.AnyInteracts;
import org.geotools.api.filter.temporal.Before;
import org.geotools.api.filter.temporal.Begins;
import org.geotools.api.filter.temporal.BegunBy;
import org.geotools.api.filter.temporal.During;
import org.geotools.api.filter.temporal.EndedBy;
import org.geotools.api.filter.temporal.Ends;
import org.geotools.api.filter.temporal.Meets;
import org.geotools.api.filter.temporal.MetBy;
import org.geotools.api.filter.temporal.OverlappedBy;
import org.geotools.api.filter.temporal.TContains;
import org.geotools.api.filter.temporal.TEquals;
import org.geotools.api.filter.temporal.TOverlaps;
import org.jspecify.annotations.Nullable;

/**
 * Converts compatible GeoTools {@link Filter} instances to Parquet {@link FilterPredicate}s for predicate push-down.
 *
 * <p>The conversion is performed by the inner {@link FilterToFilterPredicate} visitor, which implements both
 * {@link FilterVisitor} and {@link ExpressionVisitor}.
 *
 * <h4>Supported filters</h4>
 *
 * <table>
 * <caption>GeoTools filter to Parquet predicate mapping</caption>
 * <tr><th>GeoTools Filter</th><th>Parquet Predicate</th></tr>
 * <tr><td>{@code PropertyIsEqualTo}</td><td>{@code FilterApi.eq(column, value)}</td></tr>
 * <tr><td>{@code PropertyIsNotEqualTo}</td><td>{@code FilterApi.notEq(column, value)}</td></tr>
 * <tr><td>{@code PropertyIsGreaterThan}</td><td>{@code FilterApi.gt(column, value)}</td></tr>
 * <tr><td>{@code PropertyIsGreaterThanOrEqualTo}</td><td>{@code FilterApi.gtEq(column, value)}</td></tr>
 * <tr><td>{@code PropertyIsLessThan}</td><td>{@code FilterApi.lt(column, value)}</td></tr>
 * <tr><td>{@code PropertyIsLessThanOrEqualTo}</td><td>{@code FilterApi.ltEq(column, value)}</td></tr>
 * <tr><td>{@code PropertyIsBetween}</td><td>{@code FilterApi.and(gtEq(col, lower), ltEq(col, upper))}</td></tr>
 * <tr><td>{@code PropertyIsNull}</td><td>{@code FilterApi.eq(column, null)}</td></tr>
 * <tr><td>{@code And}</td><td>{@code FilterApi.and(left, right)} (folded for 3+ children)</td></tr>
 * <tr><td>{@code Or}</td><td>{@code FilterApi.or(left, right)} (folded for 3+ children)</td></tr>
 * <tr><td>{@code Not}</td><td>{@code FilterApi.not(child)}</td></tr>
 * <tr><td>{@code IncludeFilter}</td><td>{@code null} (no filter to push down)</td></tr>
 * </table>
 *
 * <p>Comparison filters accept operands in either order ({@code property op literal} or {@code literal op property});
 * the operator is swapped automatically when the literal appears first (e.g. {@code literal > property} becomes
 * {@code property < literal}).
 *
 * <h4>Value conversion</h4>
 *
 * <p>Literal values are converted to the Parquet-native type based on the target column's {@link PrimitiveType}:
 * numbers are narrowed or widened as needed, {@link java.time.LocalDate} is converted to epoch-day ({@code INT32
 * DATE}), {@link java.time.Instant} to epoch-millis ({@code INT64 TIMESTAMP}), strings to {@link Binary}, and
 * {@link java.math.BigInteger} values for deprecated {@code INT96} columns are zero-padded to 12 bytes.
 *
 * <h4>Unsupported filters</h4>
 *
 * <p>Spatial filters (BBOX, Intersects, Contains, Within, etc.), temporal filters (After, Before, During, etc.),
 * {@code PropertyIsLike}, {@code PropertyIsNil}, {@code Id}, arithmetic expressions, and {@code Function} all throw
 * {@link UnsupportedOperationException}.
 *
 * @see SchemaBuilder
 */
class FilterPredicateBuilder {

    /**
     * Converts a GeoTools {@link Filter} to a Parquet {@link FilterPredicate}.
     *
     * @param filter the GeoTools filter to convert, or {@code null}
     * @param schema the Parquet message schema, used to resolve column types for value conversion
     * @return the equivalent {@code FilterPredicate}, or {@code null} if the filter is {@code null} or
     *     {@link Filter#INCLUDE} (meaning no filtering should be applied)
     * @throws UnsupportedOperationException if the filter contains unsupported filter or expression types
     */
    public @Nullable FilterPredicate convert(@Nullable Filter filter, MessageType schema) {
        if (filter == null || filter == Filter.INCLUDE) {
            return null;
        }
        FilterToFilterPredicate mapper = new FilterToFilterPredicate(schema);
        return (FilterPredicate) filter.accept(mapper, null);
    }

    /**
     * Visitor that traverses a GeoTools {@link Filter} / {@link Expression} tree and produces the equivalent Parquet
     * {@link FilterPredicate}.
     *
     * <p>For comparison filters, the visitor extracts the {@link PropertyName} and {@link Literal} from the two
     * sub-expressions (in either order), looks up the column's {@link PrimitiveType} from the schema, creates the
     * typed Parquet column via {@link FilterApi}, converts the literal value, and delegates to the appropriate
     * {@code FilterApi} comparison method.
     */
    public static class FilterToFilterPredicate implements ExpressionVisitor, FilterVisitor {

        private final MessageType schema;

        public FilterToFilterPredicate(MessageType schema) {
            this.schema = schema;
        }

        private PrimitiveType resolvePrimitiveType(String columnPath) {
            String[] path = columnPath.split("\\.");
            return schema.getColumnDescription(path).getPrimitiveType();
        }

        /** Creates the typed Parquet filter column for the given primitive type. */
        private Column<?> createColumn(String path, PrimitiveType primitiveType) {
            return switch (primitiveType.getPrimitiveTypeName()) {
                case INT32 -> FilterApi.intColumn(path);
                case INT64 -> FilterApi.longColumn(path);
                case FLOAT -> FilterApi.floatColumn(path);
                case DOUBLE -> FilterApi.doubleColumn(path);
                case BOOLEAN -> FilterApi.booleanColumn(path);
                case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> FilterApi.binaryColumn(path);
            };
        }

        /**
         * Converts a GeoTools literal value to the Parquet-native type expected by the target column.
         *
         * <p>Handles numeric widening/narrowing, {@link LocalDate} to epoch-day, {@link Instant} to epoch-millis,
         * {@link String} to {@link Binary}, and {@link BigInteger} to 12-byte {@link Binary} for deprecated
         * {@code INT96} columns.
         */
        @SuppressWarnings("unchecked")
        private <T extends Comparable<T>> T convertValue(Object value, PrimitiveType primitiveType) {
            PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
            LogicalTypeAnnotation logical = primitiveType.getLogicalTypeAnnotation();

            Object result =
                    switch (typeName) {
                        case INT32 -> {
                            if (logical instanceof DateLogicalTypeAnnotation && value instanceof LocalDate ld) {
                                yield (int) ld.toEpochDay();
                            }
                            yield ((Number) value).intValue();
                        }
                        case INT64 -> {
                            if (logical instanceof TimestampLogicalTypeAnnotation && value instanceof Instant inst) {
                                yield inst.toEpochMilli();
                            }
                            yield ((Number) value).longValue();
                        }
                        case FLOAT -> ((Number) value).floatValue();
                        case DOUBLE -> ((Number) value).doubleValue();
                        case BOOLEAN -> (Boolean) value;
                        case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                            if (value instanceof String s) {
                                yield Binary.fromString(s);
                            } else if (value instanceof byte[] bytes) {
                                yield Binary.fromReusedByteArray(bytes);
                            } else if (value instanceof Binary b) {
                                yield b;
                            } else if (value instanceof BigInteger bi) {
                                // INT96 is deprecated; BigInteger values are zero-padded to 12 bytes
                                yield toBinary(bi, typeName == PrimitiveTypeName.INT96 ? 12 : 0);
                            }
                            yield Binary.fromString(value.toString());
                        }
                    };
            return (T) result;
        }

        /**
         * Converts a {@link BigInteger} to a {@link Binary}, optionally zero-padding or sign-extending to
         * {@code fixedWidth} bytes. A {@code fixedWidth} of 0 means no padding.
         */
        private static Binary toBinary(BigInteger bi, int fixedWidth) {
            byte[] raw = bi.toByteArray();
            if (fixedWidth > 0 && raw.length != fixedWidth) {
                byte[] padded = new byte[fixedWidth];
                // sign-extend: fill with 0xFF if negative, 0x00 if positive
                if (bi.signum() < 0) {
                    Arrays.fill(padded, (byte) 0xFF);
                }
                int srcPos = Math.max(0, raw.length - fixedWidth);
                int destPos = Math.max(0, fixedWidth - raw.length);
                int len = Math.min(raw.length, fixedWidth);
                System.arraycopy(raw, srcPos, padded, destPos, len);
                raw = padded;
            }
            return Binary.fromReusedByteArray(raw);
        }

        @FunctionalInterface
        private interface ComparisonFactory {
            FilterPredicate create(Column<?> column, Comparable<?> value);
        }

        /**
         * Extracts a {@link PropertyName} and {@link Literal} from the two expressions (in either order), resolves the
         * column type, converts the literal, and applies the appropriate comparison operator. When the literal is
         * {@code expr1}, the {@code swappedOp} is used (e.g. {@code gt} becomes {@code lt}).
         */
        private FilterPredicate buildComparison(
                Expression expr1, Expression expr2, ComparisonFactory normalOp, ComparisonFactory swappedOp) {
            PropertyName property;
            Literal literal;
            ComparisonFactory op;

            if (expr1 instanceof PropertyName pn && expr2 instanceof Literal lit) {
                property = pn;
                literal = lit;
                op = normalOp;
            } else if (expr1 instanceof Literal lit && expr2 instanceof PropertyName pn) {
                property = pn;
                literal = lit;
                op = swappedOp;
            } else {
                throw new UnsupportedOperationException("Filter comparison requires a PropertyName and a Literal, got: "
                        + expr1.getClass().getSimpleName() + " and "
                        + expr2.getClass().getSimpleName());
            }

            String path = property.getPropertyName();
            PrimitiveType primitiveType = resolvePrimitiveType(path);
            Column<?> column = createColumn(path, primitiveType);
            Comparable<?> value = convertValue(literal.getValue(), primitiveType);
            return op.create(column, value);
        }

        @SuppressWarnings("unchecked")
        private static FilterPredicate makeEq(Column<?> column, Comparable<?> value) {
            if (column instanceof IntColumn c) return FilterApi.eq(c, (Integer) value);
            if (column instanceof LongColumn c) return FilterApi.eq(c, (Long) value);
            if (column instanceof FloatColumn c) return FilterApi.eq(c, (Float) value);
            if (column instanceof DoubleColumn c) return FilterApi.eq(c, (Double) value);
            if (column instanceof BooleanColumn c) return FilterApi.eq(c, (Boolean) value);
            if (column instanceof BinaryColumn c) return FilterApi.eq(c, (Binary) value);
            throw new IllegalArgumentException("Unsupported column type: " + column.getClass());
        }

        @SuppressWarnings("unchecked")
        private static FilterPredicate makeNotEq(Column<?> column, Comparable<?> value) {
            if (column instanceof IntColumn c) return FilterApi.notEq(c, (Integer) value);
            if (column instanceof LongColumn c) return FilterApi.notEq(c, (Long) value);
            if (column instanceof FloatColumn c) return FilterApi.notEq(c, (Float) value);
            if (column instanceof DoubleColumn c) return FilterApi.notEq(c, (Double) value);
            if (column instanceof BooleanColumn c) return FilterApi.notEq(c, (Boolean) value);
            if (column instanceof BinaryColumn c) return FilterApi.notEq(c, (Binary) value);
            throw new IllegalArgumentException("Unsupported column type: " + column.getClass());
        }

        private static FilterPredicate makeGt(Column<?> column, Comparable<?> value) {
            if (column instanceof IntColumn c) return FilterApi.gt(c, (Integer) value);
            if (column instanceof LongColumn c) return FilterApi.gt(c, (Long) value);
            if (column instanceof FloatColumn c) return FilterApi.gt(c, (Float) value);
            if (column instanceof DoubleColumn c) return FilterApi.gt(c, (Double) value);
            if (column instanceof BinaryColumn c) return FilterApi.gt(c, (Binary) value);
            throw new IllegalArgumentException("Unsupported column type for gt: " + column.getClass());
        }

        private static FilterPredicate makeGtEq(Column<?> column, Comparable<?> value) {
            if (column instanceof IntColumn c) return FilterApi.gtEq(c, (Integer) value);
            if (column instanceof LongColumn c) return FilterApi.gtEq(c, (Long) value);
            if (column instanceof FloatColumn c) return FilterApi.gtEq(c, (Float) value);
            if (column instanceof DoubleColumn c) return FilterApi.gtEq(c, (Double) value);
            if (column instanceof BinaryColumn c) return FilterApi.gtEq(c, (Binary) value);
            throw new IllegalArgumentException("Unsupported column type for gtEq: " + column.getClass());
        }

        private static FilterPredicate makeLt(Column<?> column, Comparable<?> value) {
            if (column instanceof IntColumn c) return FilterApi.lt(c, (Integer) value);
            if (column instanceof LongColumn c) return FilterApi.lt(c, (Long) value);
            if (column instanceof FloatColumn c) return FilterApi.lt(c, (Float) value);
            if (column instanceof DoubleColumn c) return FilterApi.lt(c, (Double) value);
            if (column instanceof BinaryColumn c) return FilterApi.lt(c, (Binary) value);
            throw new IllegalArgumentException("Unsupported column type for lt: " + column.getClass());
        }

        private static FilterPredicate makeLtEq(Column<?> column, Comparable<?> value) {
            if (column instanceof IntColumn c) return FilterApi.ltEq(c, (Integer) value);
            if (column instanceof LongColumn c) return FilterApi.ltEq(c, (Long) value);
            if (column instanceof FloatColumn c) return FilterApi.ltEq(c, (Float) value);
            if (column instanceof DoubleColumn c) return FilterApi.ltEq(c, (Double) value);
            if (column instanceof BinaryColumn c) return FilterApi.ltEq(c, (Binary) value);
            throw new IllegalArgumentException("Unsupported column type for ltEq: " + column.getClass());
        }

        // ExpressionVisitor methods

        @Override
        public Object visit(NilExpression expression, Object unused) {
            throw new UnsupportedOperationException("NilExpression is not supported");
        }

        @Override
        public Object visit(Add expression, Object unused) {
            throw new UnsupportedOperationException("Add expression is not supported");
        }

        @Override
        public Object visit(Divide expression, Object unused) {
            throw new UnsupportedOperationException("Divide expression is not supported");
        }

        @Override
        public Object visit(Function expression, Object unused) {
            throw new UnsupportedOperationException("Function expression is not supported");
        }

        @Override
        public Object visit(Literal expression, Object unused) {
            return expression.getValue();
        }

        @Override
        public Object visit(Multiply expression, Object unused) {
            throw new UnsupportedOperationException("Multiply expression is not supported");
        }

        @Override
        public Object visit(PropertyName expression, Object unused) {
            return expression.getPropertyName();
        }

        @Override
        public Object visit(Subtract expression, Object unused) {
            throw new UnsupportedOperationException("Subtract expression is not supported");
        }

        // FilterVisitor methods

        @Override
        public @Nullable FilterPredicate visitNullFilter(Object unused) {
            return null;
        }

        @Override
        public FilterPredicate visit(ExcludeFilter filter, Object unused) {
            throw new UnsupportedOperationException("ExcludeFilter cannot be pushed down to Parquet");
        }

        @Override
        public @Nullable FilterPredicate visit(IncludeFilter filter, Object unused) {
            return null;
        }

        @Override
        public FilterPredicate visit(And filter, Object unused) {
            List<Filter> children = filter.getChildren();
            FilterPredicate result = (FilterPredicate) children.get(0).accept(this, null);
            for (int i = 1; i < children.size(); i++) {
                FilterPredicate next = (FilterPredicate) children.get(i).accept(this, null);
                result = FilterApi.and(result, next);
            }
            return result;
        }

        @Override
        public FilterPredicate visit(Id filter, Object unused) {
            throw new UnsupportedOperationException("Id filter is not supported");
        }

        @Override
        public FilterPredicate visit(Not filter, Object unused) {
            FilterPredicate child = (FilterPredicate) filter.getFilter().accept(this, null);
            return FilterApi.not(child);
        }

        @Override
        public FilterPredicate visit(Or filter, Object unused) {
            List<Filter> children = filter.getChildren();
            FilterPredicate result = (FilterPredicate) children.get(0).accept(this, null);
            for (int i = 1; i < children.size(); i++) {
                FilterPredicate next = (FilterPredicate) children.get(i).accept(this, null);
                result = FilterApi.or(result, next);
            }
            return result;
        }

        @Override
        public FilterPredicate visit(PropertyIsBetween filter, Object unused) {
            Expression valueExpr = filter.getExpression();
            if (!(valueExpr instanceof PropertyName property)) {
                throw new UnsupportedOperationException("PropertyIsBetween requires a PropertyName expression");
            }
            String path = property.getPropertyName();
            PrimitiveType primitiveType = resolvePrimitiveType(path);
            Column<?> column = createColumn(path, primitiveType);

            Comparable<?> lower = convertValue(((Literal) filter.getLowerBoundary()).getValue(), primitiveType);
            Comparable<?> upper = convertValue(((Literal) filter.getUpperBoundary()).getValue(), primitiveType);
            return FilterApi.and(makeGtEq(column, lower), makeLtEq(column, upper));
        }

        @Override
        public FilterPredicate visit(PropertyIsEqualTo filter, Object unused) {
            return buildComparison(
                    filter.getExpression1(),
                    filter.getExpression2(),
                    FilterToFilterPredicate::makeEq,
                    FilterToFilterPredicate::makeEq);
        }

        @Override
        public FilterPredicate visit(PropertyIsNotEqualTo filter, Object unused) {
            return buildComparison(
                    filter.getExpression1(),
                    filter.getExpression2(),
                    FilterToFilterPredicate::makeNotEq,
                    FilterToFilterPredicate::makeNotEq);
        }

        @Override
        public FilterPredicate visit(PropertyIsGreaterThan filter, Object unused) {
            return buildComparison(
                    filter.getExpression1(),
                    filter.getExpression2(),
                    FilterToFilterPredicate::makeGt,
                    FilterToFilterPredicate::makeLt);
        }

        @Override
        public FilterPredicate visit(PropertyIsGreaterThanOrEqualTo filter, Object unused) {
            return buildComparison(
                    filter.getExpression1(),
                    filter.getExpression2(),
                    FilterToFilterPredicate::makeGtEq,
                    FilterToFilterPredicate::makeLtEq);
        }

        @Override
        public FilterPredicate visit(PropertyIsLessThan filter, Object unused) {
            return buildComparison(
                    filter.getExpression1(),
                    filter.getExpression2(),
                    FilterToFilterPredicate::makeLt,
                    FilterToFilterPredicate::makeGt);
        }

        @Override
        public FilterPredicate visit(PropertyIsLessThanOrEqualTo filter, Object unused) {
            return buildComparison(
                    filter.getExpression1(),
                    filter.getExpression2(),
                    FilterToFilterPredicate::makeLtEq,
                    FilterToFilterPredicate::makeGtEq);
        }

        @Override
        public FilterPredicate visit(PropertyIsLike filter, Object unused) {
            throw new UnsupportedOperationException("PropertyIsLike is not supported");
        }

        @Override
        public FilterPredicate visit(PropertyIsNull filter, Object unused) {
            Expression expr = filter.getExpression();
            if (!(expr instanceof PropertyName property)) {
                throw new UnsupportedOperationException("PropertyIsNull requires a PropertyName expression");
            }
            String path = property.getPropertyName();
            PrimitiveType primitiveType = resolvePrimitiveType(path);
            Column<?> column = createColumn(path, primitiveType);
            return makeEqNull(column);
        }

        private static FilterPredicate makeEqNull(Column<?> column) {
            if (column instanceof IntColumn c) return FilterApi.eq(c, null);
            if (column instanceof LongColumn c) return FilterApi.eq(c, null);
            if (column instanceof FloatColumn c) return FilterApi.eq(c, null);
            if (column instanceof DoubleColumn c) return FilterApi.eq(c, null);
            if (column instanceof BooleanColumn c) return FilterApi.eq(c, null);
            if (column instanceof BinaryColumn c) return FilterApi.eq(c, null);
            throw new IllegalArgumentException("Unsupported column type: " + column.getClass());
        }

        @Override
        public FilterPredicate visit(PropertyIsNil filter, Object unused) {
            throw new UnsupportedOperationException("PropertyIsNil is not supported");
        }

        @Override
        public FilterPredicate visit(BBOX filter, Object unused) {
            throw new UnsupportedOperationException("BBOX spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Beyond filter, Object unused) {
            throw new UnsupportedOperationException("Beyond spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Contains filter, Object unused) {
            throw new UnsupportedOperationException("Contains spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Crosses filter, Object unused) {
            throw new UnsupportedOperationException("Crosses spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Disjoint filter, Object unused) {
            throw new UnsupportedOperationException("Disjoint spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(DWithin filter, Object unused) {
            throw new UnsupportedOperationException("DWithin spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Equals filter, Object unused) {
            throw new UnsupportedOperationException("Equals spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Intersects filter, Object unused) {
            throw new UnsupportedOperationException("Intersects spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Overlaps filter, Object unused) {
            throw new UnsupportedOperationException("Overlaps spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Touches filter, Object unused) {
            throw new UnsupportedOperationException("Touches spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(Within filter, Object unused) {
            throw new UnsupportedOperationException("Within spatial filter is not supported");
        }

        @Override
        public FilterPredicate visit(After after, Object unused) {
            throw new UnsupportedOperationException("After temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(AnyInteracts anyInteracts, Object unused) {
            throw new UnsupportedOperationException("AnyInteracts temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(Before before, Object unused) {
            throw new UnsupportedOperationException("Before temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(Begins begins, Object unused) {
            throw new UnsupportedOperationException("Begins temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(BegunBy begunBy, Object unused) {
            throw new UnsupportedOperationException("BegunBy temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(During during, Object unused) {
            throw new UnsupportedOperationException("During temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(EndedBy endedBy, Object unused) {
            throw new UnsupportedOperationException("EndedBy temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(Ends ends, Object unused) {
            throw new UnsupportedOperationException("Ends temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(Meets meets, Object unused) {
            throw new UnsupportedOperationException("Meets temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(MetBy metBy, Object unused) {
            throw new UnsupportedOperationException("MetBy temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(OverlappedBy overlappedBy, Object unused) {
            throw new UnsupportedOperationException("OverlappedBy temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(TContains contains, Object unused) {
            throw new UnsupportedOperationException("TContains temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(TEquals equals, Object unused) {
            throw new UnsupportedOperationException("TEquals temporal filter is not supported");
        }

        @Override
        public FilterPredicate visit(TOverlaps contains, Object unused) {
            throw new UnsupportedOperationException("TOverlaps temporal filter is not supported");
        }
    }
}
