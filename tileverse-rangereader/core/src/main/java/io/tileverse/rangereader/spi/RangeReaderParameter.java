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
package io.tileverse.rangereader.spi;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Optional;

/**
 * Represents a configurable parameter for a {@link RangeReaderProvider}.
 * This record provides metadata about a parameter, including its key, description, type,
 * default value, and a list of sample values.
 *
 * @param <T> The type of the parameter's value.
 * @param key The unique key identifying the parameter.
 * @param title A human-readable title of the parameter.
 * @param description A human-readable description of the parameter.
 * @param group A logical grouping for the parameter (e.g., "caching", "s3", "gcs", etc.).
 * @param subgroup An optional logical sub group for the parameter (e.g., "authentication", "advanced", etc.).
 * @param type The {@link Class} representing the type of the parameter's value.
 * @param defaultValue An {@link Optional} containing the default value of the parameter, if any.
 * @param sampleValues A list of sample or suggested values for the parameter.
 */
public record RangeReaderParameter<T>(
        String key,
        String title,
        String description,
        String group,
        Optional<String> subgroup,
        Class<T> type,
        Optional<T> defaultValue,
        List<T> sampleValues,
        boolean password) {

    /** Standard parameter group for caching-related configuration. */
    public static final String GROUP_CACHING = "caching";
    /** Standard parameter subgroup for authentication-related configuration. */
    public static final String SUBGROUP_AUTHENTICATION = "authentication";

    /**
     * Compact constructor for {@link RangeReaderParameter} that performs validation.
     *
     * @param key unique identifier for this parameter
     * @param title human-readable display name for this parameter
     * @param description detailed description of what this parameter does
     * @param group logical grouping category for organizing parameters
     * @param subgroup sub-category within the group for finer organization
     * @param type the Java type of values this parameter accepts
     * @param defaultValue optional default value for this parameter
     * @param sampleValues list of example values to help users understand valid inputs
     * @param password metadata about whether the parameter should be treated as password by the client code (e.g. mask it in user interfaces)
     */
    public RangeReaderParameter {
        requireNonNull(key, "Parameter key cannot be null");
        requireNonNull(title, "Parameter title cannot be null");
        requireNonNull(description, "Parameter description cannot be null");
        requireNonNull(group, "Parameter group cannot be null");
        requireNonNull(subgroup, "Parameter subgroup cannot be null");
        requireNonNull(type, "Parameter type cannot be null");
        requireNonNull(defaultValue, "Parameter default value optional cannot be null");
        sampleValues = List.copyOf(requireNonNull(sampleValues, "Parameter sample values list cannot be null"));
    }

    /**
     * Creates a new {@link Builder} for constructing a {@link RangeReaderParameter}.
     *
     * @return A new {@link Builder} instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link RangeReaderParameter}.
     */
    public static class Builder {
        String key;
        String title;
        String description;
        String group;
        String subgroup;
        boolean password;

        @SuppressWarnings("rawtypes")
        Class type;

        Optional<Object> defaultValue = Optional.empty(); // Initialize with empty optional
        List<Object> sampleValues = List.of();

        /**
         * Creates a new {@code Builder}.
         */
        public Builder() {
            // Default constructor
        }

        /**
         * Sets the unique key for the parameter.
         *
         * @param key The parameter key.
         * @return This builder instance.
         */
        public Builder key(String key) {
            this.key = key;
            return this;
        }
        /**
         * Sets the human-readable title for the parameter.
         *
         * @param title The parameter title.
         * @return This builder instance.
         */
        public Builder title(String title) {
            this.title = title;
            return this;
        }

        /**
         * Sets the human-readable description for the parameter.
         *
         * @param description The parameter description.
         * @return This builder instance.
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the type of the parameter's value.
         *
         * @param type The {@link Class} representing the parameter's type.
         * @return This builder instance.
         */
        public Builder type(Class<?> type) {
            this.type = type;
            return this;
        }

        /**
         * Sets the logical grouping for the parameter.
         *
         * @param group The parameter group.
         * @return This builder instance.
         */
        public Builder group(String group) {
            this.group = group;
            return this;
        }

        /**
         * Sets the logical sub-grouping for the parameter (e.g. "caching", "authentication").
         *
         * @param subgroup optional subgroup parameter
         * @return This builder instance.
         */
        public Builder subgroup(String subgroup) {
            this.subgroup = group;
            return this;
        }

        /**
         * Sets the default value for the parameter.
         *
         * @param defaultValue The default value.
         * @return This builder instance.
         */
        public Builder defaultValue(Object defaultValue) {
            this.defaultValue = Optional.ofNullable(defaultValue);
            return this;
        }

        /**
         * Sets a list of sample or suggested values for the parameter.
         *
         * @param values An array of sample values.
         * @return This builder instance.
         */
        public Builder options(Object... values) {
            this.sampleValues = values == null || values.length == 0 ? List.of() : List.of(values);
            return this;
        }

        /**
         * Sets whether the parameter is a "password" field
         *
         * @param isPassword true if the parameter should be treated as password by the client code (e.g. mask it in user interfaces)
         * @return This builder instance.
         */
        public Builder password(boolean isPassword) {
            this.password = isPassword;
            return this;
        }

        /**
         * Builds a new {@link RangeReaderParameter} instance.
         *
         * @param <T> The type of the parameter value.
         * @return A new {@link RangeReaderParameter}.
         * @throws NullPointerException if key, description, group, or type is {@code null}.
         */
        @SuppressWarnings("unchecked")
        public <T> RangeReaderParameter<T> build() {
            return new RangeReaderParameter<>(
                    key,
                    title,
                    description,
                    group,
                    Optional.ofNullable(subgroup),
                    type,
                    defaultValue,
                    sampleValues,
                    password);
        }
    }
}
