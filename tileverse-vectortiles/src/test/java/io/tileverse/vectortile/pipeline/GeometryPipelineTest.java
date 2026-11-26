/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.vectortile.pipeline;

import static io.tileverse.vectortile.mvt.Geometries.wkt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;

class GeometryPipelineTest {

    @Test
    void emptyPipelineHandsGeometryToSink() {
        Geometry point = wkt("POINT (1 1)");

        List<Geometry> written = run(GeometryPipeline.empty(), point);

        assertThat(written).singleElement().isSameAs(point);
    }

    // The steps never see the dropped geometry either
    @Test
    void dropsNullAndEmptyGeometry() {
        List<Geometry> stepInputs = new ArrayList<>();
        GeometryPipeline pipeline = GeometryPipeline.of(recording(stepInputs, UnaryOperator.identity()));
        List<Geometry> written = new ArrayList<>();

        pipeline.run(null, written::add);
        pipeline.run(wkt("POLYGON EMPTY"), written::add);

        assertThat(stepInputs).isEmpty();
        assertThat(written).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("collections")
    void splitsCollectionMembers(String collection, List<String> expectedMembers) {
        List<Geometry> written = run(GeometryPipeline.empty(), wkt(collection));

        assertThat(texts(written)).containsExactlyElementsOf(expectedMembers);
    }

    static Stream<Arguments> collections() {
        return Stream.of(
                arguments(
                        "GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (0 0, 9 9), POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0)))",
                        List.of("POINT (1 1)", "LINESTRING (0 0, 9 9)", "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))")),
                arguments(
                        "GEOMETRYCOLLECTION (POINT (1 1), GEOMETRYCOLLECTION (POINT (2 2), LINESTRING (0 0, 9 9)))",
                        List.of("POINT (1 1)", "POINT (2 2)", "LINESTRING (0 0, 9 9)")),
                arguments("MULTIPOINT ((1 1), (2 2))", List.of("MULTIPOINT ((1 1), (2 2))")));
    }

    @Test
    void membersContinueThroughRemainingSteps() {
        Geometry twoPoints = wkt("GEOMETRYCOLLECTION (POINT (1 1), POINT (2 2))");
        List<Geometry> firstInputs = new ArrayList<>();
        List<Geometry> secondInputs = new ArrayList<>();
        GeometryStep split = recording(firstInputs, geometry -> twoPoints);
        GeometryStep keep = recording(secondInputs, UnaryOperator.identity());

        List<Geometry> written = run(GeometryPipeline.of(split, keep), wkt("POINT (5 5)"));

        assertThat(firstInputs).hasSize(1);
        assertThat(texts(secondInputs)).containsExactly("POINT (1 1)", "POINT (2 2)");
        assertThat(texts(written)).containsExactly("POINT (1 1)", "POINT (2 2)");
    }

    @Test
    void nullAndEmptyResultsDropFeature() {
        Geometry empty = wkt("POINT EMPTY");
        List<Geometry> nextInputs = new ArrayList<>();
        GeometryStep next = recording(nextInputs, UnaryOperator.identity());
        GeometryPipeline toNull = GeometryPipeline.of(geometry -> null, next);
        GeometryPipeline toEmpty = GeometryPipeline.of(geometry -> empty, next);
        List<Geometry> written = new ArrayList<>();

        toNull.run(wkt("POINT (1 1)"), written::add);
        toEmpty.run(wkt("POINT (1 1)"), written::add);

        assertThat(nextInputs).isEmpty();
        assertThat(written).isEmpty();
    }

    @Test
    void rejectsNullStep() {
        GeometryStep identity = geometry -> geometry;

        assertThatThrownBy(() -> GeometryPipeline.of(identity, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void listsStepsInOrder() {
        SizeFilterStep sizeFilter = new SizeFilterStep(1, 1);
        ClipStep clip = new ClipStep(4096, 64);

        GeometryPipeline pipeline = GeometryPipeline.of(sizeFilter, clip);

        String expected = """
                GeometryPipeline[SizeFilterStep[minimumLength=1.0, minimumArea=1.0], \
                ClipStep[extent=4096.0, buffer=64.0]]""";
        assertThat(pipeline.steps()).containsExactly(sizeFilter, clip);
        assertThat(pipeline).hasToString(expected);
    }

    // Neither the list returned by steps() nor the array given to of() can change the steps
    @Test
    void stepsAreUnmodifiable() {
        GeometryStep identity = geometry -> geometry;
        GeometryStep[] given = {identity};
        GeometryPipeline pipeline = GeometryPipeline.of(given);
        List<GeometryStep> steps = pipeline.steps();

        given[0] = geometry -> null;

        assertThat(steps).containsExactly(identity);
        assertThatThrownBy(() -> steps.add(identity)).isInstanceOf(UnsupportedOperationException.class);
    }

    /** Runs the pipeline and returns the geometries handed to the sink. */
    private static List<Geometry> run(GeometryPipeline pipeline, Geometry geometry) {
        List<Geometry> written = new ArrayList<>();
        pipeline.run(geometry, written::add);
        return written;
    }

    /** A step adding its input to the list, then returning what the function makes of it. */
    private static GeometryStep recording(List<Geometry> inputs, UnaryOperator<Geometry> result) {
        return geometry -> {
            inputs.add(geometry);
            return result.apply(geometry);
        };
    }

    private static List<String> texts(List<Geometry> geometries) {
        return geometries.stream().map(Geometry::toText).toList();
    }
}
