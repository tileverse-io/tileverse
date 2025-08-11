/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.vectortile.mvt;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.Envelope;

/**
 * A forwarding sub-sequence coordinate sequence
 */
final class SubCoordinateSequence implements CoordinateSequence {
    private final CoordinateSequence delegate;
    private final int start;
    private final int length;
    private final CoordinateSequenceFactory fac;

    /**
     * @param delegate the delegate sequence
     * @param start
     * @param length
     * @param fac the delegate sequence factory to use on {@link #copy()}
     */
    public SubCoordinateSequence(CoordinateSequence delegate, int start, int length, CoordinateSequenceFactory fac) {
        this.fac = fac;
        if (delegate == null) {
            throw new IllegalArgumentException("Delegate coordinate sequence cannot be null");
        }
        if (start < 0) {
            throw new IllegalArgumentException("Start index cannot be negative: " + start);
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }
        if (start + length > delegate.size()) {
            throw new IllegalArgumentException("Start + lenght (" + (start + length)
                    + ") cannot be greater than delegate's size: " + delegate.size());
        }
        this.delegate = delegate;
        this.start = start;
        this.length = length;
    }

    @Override
    public int size() {
        return length;
    }

    @Override
    public int getDimension() {
        return delegate.getDimension();
    }

    @Override
    public Coordinate getCoordinate(int pos) {
        checkBounds(pos);
        final int realPos = start + pos;
        Coordinate c = delegate.createCoordinate();
        c.setX(delegate.getOrdinate(realPos, 0));
        c.setY(delegate.getOrdinate(realPos, 1));
        return c;
    }

    private final void checkBounds(int pos) {
        if (pos < 0 || pos >= length) {
            throw new IndexOutOfBoundsException("Position " + pos + " is out of bounds [0, " + length + ")");
        }
    }

    @Override
    public Coordinate getCoordinateCopy(int pos) {
        checkBounds(pos);
        return delegate.getCoordinateCopy(start + pos);
    }

    @Override
    public void getCoordinate(int pos, Coordinate coord) {
        checkBounds(pos);
        delegate.getCoordinate(start + pos, coord);
    }

    @Override
    public double getX(int pos) {
        checkBounds(pos);
        return delegate.getX(start + pos);
    }

    @Override
    public double getY(int pos) {
        checkBounds(pos);
        return delegate.getY(start + pos);
    }

    @Override
    public double getZ(int pos) {
        checkBounds(pos);
        return delegate.getZ(start + pos);
    }

    @Override
    public double getM(int pos) {
        checkBounds(pos);
        return delegate.getM(start + pos);
    }

    @Override
    public double getOrdinate(int pos, int ordinateIndex) {
        checkBounds(pos);
        return delegate.getOrdinate(start + pos, ordinateIndex);
    }

    @Override
    public void setOrdinate(int index, int ordinateIndex, double value) {
        checkBounds(index);
        delegate.setOrdinate(start + index, ordinateIndex, value);
    }

    @Override
    public Coordinate[] toCoordinateArray() {
        Coordinate[] coords = new Coordinate[length];
        for (int i = 0; i < length; i++) {
            coords[i] = getCoordinate(i);
        }
        return coords;
    }

    @Override
    public Envelope expandEnvelope(Envelope env) {
        for (int i = 0; i < length; i++) {
            env.expandToInclude(getX(i), getY(i));
        }
        return env;
    }

    @Override
    public CoordinateSequence copy() {
        CoordinateSequence copy = fac.create(length, 2);
        for (int i = 0, d = start; i < length; i++, d++) {
            copy.setOrdinate(i, Coordinate.X, delegate.getOrdinate(d, Coordinate.X));
            copy.setOrdinate(i, Coordinate.Y, delegate.getOrdinate(d, Coordinate.Y));
        }
        return copy;
    }

    @Override
    @Deprecated
    public Object clone() {
        throw new UnsupportedOperationException();
    }
}
