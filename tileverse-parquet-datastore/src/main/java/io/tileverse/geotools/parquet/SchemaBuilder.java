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

import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeographyLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeometryLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.FeatureTypeFactory;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.logging.Logging;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.locationtech.jts.geom.Geometry;

/**
 * Converts a Parquet {@link MessageType} schema to a GeoTools {@link SimpleFeatureType}.
 *
 * <p>The schema is traversed recursively. Primitive fields become attributes whose Java binding class is determined by a
 * two-step process:
 *
 * <ol>
 *   <li>If the column carries a {@link LogicalTypeAnnotation}, it is resolved via {@link BindingClassVisitor} (e.g.
 *       {@code STRING -> String}, {@code DATE -> LocalDate}, {@code TIMESTAMP(UTC) -> Instant}, {@code GEOMETRY ->
 *       Geometry}).
 *   <li>Otherwise, the {@linkplain #defaultBinding(PrimitiveTypeName) primitive-type default} is used (e.g. {@code
 *       INT32 -> Integer}, {@code BINARY -> byte[]}).
 * </ol>
 *
 * <p>Group types are handled as follows:
 *
 * <ul>
 *   <li><strong>LIST</strong>-annotated groups become a single attribute bound to {@link List}.
 *   <li><strong>MAP</strong>-annotated groups become a single attribute bound to {@link Map}.
 *   <li><strong>Plain structs</strong> (no logical type annotation) are flattened: their children are recursively
 *       traversed with dot-joined attribute names (e.g. {@code bbox.xmin}).
 * </ul>
 *
 * <p>Column repetition controls nillability: {@code REQUIRED -> nillable(false)}, {@code OPTIONAL -> nillable(true)}.
 *
 * <p>Geometry and Geography columns are registered with a {@link CoordinateReferenceSystem} extracted from the logical
 * type annotation's CRS metadata, falling back to {@link DefaultGeographicCRS#WGS84 WGS84} when absent or unparseable.
 *
 * @see BindingClassVisitor
 * @see FilterPredicateBuilder
 */
class SchemaBuilder {

    private static final Logger LOGGER = Logging.getLogger(SchemaBuilder.class);

    private FeatureTypeFactory ftf;

    public SchemaBuilder() {
        this(CommonFactoryFinder.getFeatureTypeFactory(null));
    }

    public SchemaBuilder(@NonNull FeatureTypeFactory ftf) {
        this.ftf = requireNonNull(ftf);
    }

    /**
     * Builds a {@link SimpleFeatureType} from the given Parquet schema.
     *
     * @param typeName the name to assign to the resulting feature type
     * @param parquetSchema the Parquet message schema to convert
     * @return a {@code SimpleFeatureType} whose attributes mirror the Parquet columns
     */
    public SimpleFeatureType build(String typeName, MessageType parquetSchema) {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder(ftf);
        builder.setName(typeName);

        for (Type field : parquetSchema.getFields()) {
            addField(field, "", builder);
        }

        return builder.buildFeatureType();
    }

    private void addField(Type field, String prefix, SimpleFeatureTypeBuilder builder) {
        String name = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
        boolean nillable = field.getRepetition() != Repetition.REQUIRED;

        if (field.isPrimitive()) {
            addPrimitiveAttribute(field.asPrimitiveType(), name, nillable, builder);
        } else {
            GroupType group = field.asGroupType();
            LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();

            if (annotation instanceof ListLogicalTypeAnnotation) {
                builder.nillable(nillable);
                builder.add(name, List.class);
            } else if (annotation instanceof MapLogicalTypeAnnotation) {
                builder.nillable(nillable);
                builder.add(name, Map.class);
            } else {
                // Plain struct: recurse into children, flattening with dot-joined names
                for (Type child : group.getFields()) {
                    addField(child, name, builder);
                }
            }
        }
    }

    private void addPrimitiveAttribute(
            PrimitiveType primitiveType, String name, boolean nillable, SimpleFeatureTypeBuilder builder) {
        LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();

        builder.nillable(nillable);

        Class<?> binding;
        if (logicalTypeAnnotation != null) {
            BindingClassVisitor visitor = new BindingClassVisitor();
            binding =
                    logicalTypeAnnotation.accept(visitor).orElse(defaultBinding(primitiveType.getPrimitiveTypeName()));
        } else {
            binding = defaultBinding(primitiveType.getPrimitiveTypeName());
        }

        if (Geometry.class.isAssignableFrom(binding)) {
            CoordinateReferenceSystem crs = extractCrs(logicalTypeAnnotation);
            builder.add(name, binding, crs);
        } else {
            builder.add(name, binding);
        }
    }

    private static @Nullable CoordinateReferenceSystem extractCrs(@Nullable LogicalTypeAnnotation annotation) {
        String crsCode = null;
        if (annotation instanceof GeometryLogicalTypeAnnotation geo) {
            crsCode = geo.getCrs();
        } else if (annotation instanceof GeographyLogicalTypeAnnotation geo) {
            crsCode = geo.getCrs();
        }
        return decodeCrs(crsCode);
    }

    /**
     * Decodes a CRS identifier string into a {@link CoordinateReferenceSystem}.
     *
     * <p>Attempts {@link CRS#decode(String, boolean) CRS.decode} first, then {@link CRS#parseWKT(String)
     * CRS.parseWKT} as a fallback. Returns {@link DefaultGeographicCRS#WGS84 WGS84} if the code is {@code null},
     * empty, or cannot be parsed.
     */
    static CoordinateReferenceSystem decodeCrs(@Nullable String code) {
        if (code == null || code.isEmpty()) {
            return DefaultGeographicCRS.WGS84;
        }
        try {
            return CRS.decode(code, true);
        } catch (FactoryException e) {
            LOGGER.log(Level.FINE, "CRS.decode failed for ''{0}'', trying parseWKT", code);
        }
        try {
            return CRS.parseWKT(code);
        } catch (FactoryException e) {
            LOGGER.log(Level.WARNING, "Could not parse CRS ''{0}'', falling back to WGS84", code);
        }
        return DefaultGeographicCRS.WGS84;
    }

    /**
     * Returns the default Java binding for a Parquet primitive type when no logical type annotation is present.
     *
     * <p>Note: {@link PrimitiveTypeName#INT96 INT96} is a deprecated Parquet type formerly used by Impala and Hive to
     * store timestamps. It is mapped to {@link BigInteger} as the closest general-purpose Java type for a 96-bit
     * integer value.
     */
    static Class<?> defaultBinding(PrimitiveTypeName primitiveTypeName) {
        return switch (primitiveTypeName) {
            case BOOLEAN -> Boolean.class;
            case INT32 -> Integer.class;
            case INT64 -> Long.class;
            case FLOAT -> Float.class;
            case DOUBLE -> Double.class;
            case BINARY -> byte[].class;
            case FIXED_LEN_BYTE_ARRAY -> byte[].class;
            // INT96 is deprecated; mapped to BigInteger as a general-purpose 96-bit integer representation
            case INT96 -> BigInteger.class;
        };
    }

    /**
     * Resolves the Java binding class for a Parquet {@link LogicalTypeAnnotation} using the visitor pattern.
     *
     * <p>Returns {@link Optional#empty()} for unrecognized logical types, allowing the caller to fall back to the
     * {@linkplain #defaultBinding(PrimitiveTypeName) primitive-type default}.
     *
     * <h4>Supported mappings</h4>
     *
     * <table>
     * <caption>Logical type to Java binding</caption>
     * <tr><th>Logical Type</th><th>Java Binding</th></tr>
     * <tr><td>STRING, ENUM, JSON</td><td>{@code String}</td></tr>
     * <tr><td>DECIMAL</td><td>{@code BigDecimal}</td></tr>
     * <tr><td>DATE</td><td>{@code LocalDate}</td></tr>
     * <tr><td>TIME</td><td>{@code LocalTime}</td></tr>
     * <tr><td>TIMESTAMP (UTC)</td><td>{@code Instant}</td></tr>
     * <tr><td>TIMESTAMP (non-UTC)</td><td>{@code LocalDateTime}</td></tr>
     * <tr><td>INT (signed 8/16/32/64-bit)</td><td>{@code Byte / Short / Integer / Long}</td></tr>
     * <tr><td>INT (unsigned 8/16-bit)</td><td>{@code Integer}</td></tr>
     * <tr><td>INT (unsigned 32-bit)</td><td>{@code Long}</td></tr>
     * <tr><td>INT (unsigned 64-bit)</td><td>{@code BigInteger}</td></tr>
     * <tr><td>UUID</td><td>{@code UUID}</td></tr>
     * <tr><td>GEOMETRY, GEOGRAPHY</td><td>{@code Geometry} (JTS)</td></tr>
     * <tr><td>LIST</td><td>{@code List}</td></tr>
     * <tr><td>MAP</td><td>{@code Map}</td></tr>
     * </table>
     */
    static class BindingClassVisitor implements LogicalTypeAnnotationVisitor<Class<?>> {

        @Override
        public Optional<Class<?>> visit(StringLogicalTypeAnnotation stringLogicalType) {
            return Optional.of(String.class);
        }

        @Override
        public Optional<Class<?>> visit(EnumLogicalTypeAnnotation enumLogicalType) {
            return Optional.of(String.class);
        }

        @Override
        public Optional<Class<?>> visit(JsonLogicalTypeAnnotation jsonLogicalType) {
            return Optional.of(String.class);
        }

        @Override
        public Optional<Class<?>> visit(DecimalLogicalTypeAnnotation decimalLogicalType) {
            return Optional.of(BigDecimal.class);
        }

        @Override
        public Optional<Class<?>> visit(DateLogicalTypeAnnotation dateLogicalType) {
            return Optional.of(LocalDate.class);
        }

        @Override
        public Optional<Class<?>> visit(TimeLogicalTypeAnnotation timeLogicalType) {
            return Optional.of(LocalTime.class);
        }

        @Override
        public Optional<Class<?>> visit(TimestampLogicalTypeAnnotation timestampLogicalType) {
            if (timestampLogicalType.isAdjustedToUTC()) {
                return Optional.of(Instant.class);
            }
            return Optional.of(LocalDateTime.class);
        }

        @Override
        public Optional<Class<?>> visit(IntLogicalTypeAnnotation intLogicalType) {
            int bitWidth = intLogicalType.getBitWidth();
            boolean signed = intLogicalType.isSigned();
            if (signed) {
                return switch (bitWidth) {
                    case 8 -> Optional.of(Byte.class);
                    case 16 -> Optional.of(Short.class);
                    case 32 -> Optional.of(Integer.class);
                    case 64 -> Optional.of(Long.class);
                    default -> Optional.empty();
                };
            } else {
                return switch (bitWidth) {
                    case 8, 16 -> Optional.of(Integer.class);
                    case 32 -> Optional.of(Long.class);
                    case 64 -> Optional.of(BigInteger.class);
                    default -> Optional.empty();
                };
            }
        }

        @Override
        public Optional<Class<?>> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
            return Optional.of(UUID.class);
        }

        @Override
        public Optional<Class<?>> visit(GeometryLogicalTypeAnnotation geometryLogicalType) {
            return Optional.of(Geometry.class);
        }

        @Override
        public Optional<Class<?>> visit(GeographyLogicalTypeAnnotation geographyLogicalType) {
            return Optional.of(Geometry.class);
        }

        @Override
        public Optional<Class<?>> visit(ListLogicalTypeAnnotation listLogicalType) {
            return Optional.of(List.class);
        }

        @Override
        public Optional<Class<?>> visit(MapLogicalTypeAnnotation mapLogicalType) {
            return Optional.of(Map.class);
        }
    }
}
