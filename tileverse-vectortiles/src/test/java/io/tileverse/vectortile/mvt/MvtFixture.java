/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.mvt;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Coordinate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * A fixture of mapbox/mvt-fixtures, from the test resources: a tile, its content as JSON, and its validity. The
 * expected layers and features come from the JSON content, without the decoder under test.
 */
final class MvtFixture {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String FIXTURES = "/mvt-fixtures/fixtures";

    // command ids of the specification
    private static final long MOVE_TO = 1;
    private static final long CLOSE_PATH = 7;

    /** A layer as described by the fixture. */
    record ExpectedLayer(String name, int extent, List<ExpectedFeature> features) {}

    /**
     * A feature as described by the fixture. The geometry type gives the dimension of the decoded geometry: -1 for an
     * unknown type and an empty collection, 0 for points, 1 for lines, 2 for polygons.
     */
    record ExpectedFeature(long id, Map<String, Object> attributes, int dimension, List<Coordinate> coordinates) {}

    private final String id;
    private final byte[] tile;
    private final JsonNode info;
    private final JsonNode content;

    private MvtFixture(String id, byte[] tile, JsonNode info, JsonNode content) {
        this.id = id;
        this.tile = tile;
        this.info = info;
        this.content = content;
    }

    static Stream<MvtFixture> all() {
        try (Stream<Path> directories = Files.list(resource(FIXTURES))) {
            List<MvtFixture> fixtures =
                    directories.sorted().map(MvtFixture::load).toList();
            return fixtures.stream();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static MvtFixture load(String id) {
        return load(resource(FIXTURES + "/" + id));
    }

    String id() {
        return id;
    }

    byte[] tile() {
        return tile;
    }

    /** Whether the fixture marks its tile valid against version 1 or 2 of the specification. */
    boolean valid() {
        JsonNode validity = info.path("validity");
        return validity.path("v1").asBoolean(false) || validity.path("v2").asBoolean(false);
    }

    List<ExpectedLayer> expectedLayers() {
        List<ExpectedLayer> layers = new ArrayList<>();
        for (JsonNode layer : content.path("layers")) {
            layers.add(expectedLayer(layer));
        }
        return layers;
    }

    @Override
    public String toString() {
        return id + " " + info.path("description").asString("");
    }

    private static ExpectedLayer expectedLayer(JsonNode layer) {
        List<ExpectedFeature> features = new ArrayList<>();
        for (JsonNode feature : layer.path("features")) {
            features.add(expectedFeature(layer, feature));
        }
        return new ExpectedLayer(layer.path("name").asString(), layer.path("extent").asInt(4096), features);
    }

    private static ExpectedFeature expectedFeature(JsonNode layer, JsonNode feature) {
        int type = feature.path("type").asInt(0);
        List<Coordinate> coordinates = type == 0 ? List.of() : coordinates(feature.path("geometry"));
        return new ExpectedFeature(
                feature.path("id").asLong(0), attributes(layer, feature.path("tags")), type - 1, coordinates);
    }

    /** The tags are pairs of indexes into the keys and the values of the layer. */
    private static Map<String, Object> attributes(JsonNode layer, JsonNode tags) {
        Map<String, Object> attributes = new HashMap<>();
        for (int i = 0; i < tags.size(); i += 2) {
            String key = layer.path("keys").get(tags.get(i).asInt()).asString();
            JsonNode value = layer.path("values").get(tags.get(i + 1).asInt());
            attributes.put(key, value(value));
        }
        return attributes;
    }

    /** A value holds one field, named after its type. */
    private static Object value(JsonNode value) {
        Map.Entry<String, JsonNode> field = value.properties().iterator().next();
        JsonNode node = field.getValue();
        return switch (field.getKey()) {
            case "string_value" -> node.asString();
            case "float_value" -> Float.valueOf(node.floatValue());
            case "double_value" -> Double.valueOf(node.doubleValue());
            case "int_value", "sint_value" -> Long.valueOf(node.longValue());
            case "uint_value" -> Long.valueOf(node.bigIntegerValue().longValue());
            case "bool_value" -> Boolean.valueOf(node.booleanValue());
            default -> throw new IllegalArgumentException("Unknown value type " + field.getKey());
        };
    }

    /**
     * Replays the MoveTo, LineTo and ClosePath commands of the geometry. The cursor uses 32-bit integers, as the
     * specification does, and a ClosePath repeats the position of the last MoveTo.
     */
    private static List<Coordinate> coordinates(JsonNode geometry) {
        List<Coordinate> coordinates = new ArrayList<>();
        int x = 0;
        int y = 0;
        Coordinate partStart = null;
        int index = 0;
        while (index < geometry.size()) {
            long command = geometry.get(index++).asLong();
            long commandId = command & 0x7;
            long count = command >>> 3;
            if (commandId == CLOSE_PATH) {
                coordinates.add(partStart.copy());
                continue;
            }
            for (long k = 0; k < count; k++) {
                x += zigZag(geometry.get(index++).asLong());
                y += zigZag(geometry.get(index++).asLong());
                Coordinate coordinate = new Coordinate(x, y);
                if (commandId == MOVE_TO) {
                    partStart = coordinate;
                }
                coordinates.add(coordinate);
            }
        }
        return coordinates;
    }

    private static int zigZag(long parameter) {
        int value = (int) parameter;
        return (value >>> 1) ^ -(value & 1);
    }

    private static MvtFixture load(Path directory) {
        try {
            byte[] tile = Files.readAllBytes(directory.resolve("tile.mvt"));
            JsonNode info = JSON.readTree(Files.readString(directory.resolve("info.json")));
            JsonNode content = JSON.readTree(Files.readString(directory.resolve("tile.json")));
            return new MvtFixture(directory.getFileName().toString(), tile, info, content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Path resource(String name) {
        URL url = Objects.requireNonNull(MvtFixture.class.getResource(name), name);
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }
}
