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

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * Represents the configuration for creating a {@link io.tileverse.rangereader.RangeReader} instance.
 * This class holds the URI of the resource to be read, an optional explicit provider ID,
 * and a map of generic parameters that can be used by {@link RangeReaderProvider} implementations.
 */
public class RangeReaderConfig {

    /**
     * The key used in {@link Properties} to specify the URI of the resource.
     */
    public static final String URI_KEY = "io.tileverse.rangereader.uri";

    /**
     * The key used in {@link Properties} to specify the ID of a {@link RangeReaderProvider}.
     * This can be used to force the use of a specific provider when URI-based disambiguation is not sufficient.
     */
    public static final String PROVIDER_ID_KEY = "io.tileverse.rangereader.provider";

    /**
     * A parameter that can be used by client code to force a given {@link #providerId(String) provider id}
     * using {@link #setParameter(RangeReaderParameter, Object)} or {@link #setParameter(String, Object)}, and will be
     * parsed into {@link #providerId} by {@link #fromProperties(Properties)}
     */
    public static final RangeReaderParameter<String> FORCE_PROVIDER_ID = RangeReaderParameter.builder()
            .key(PROVIDER_ID_KEY)
            .title("Select range reader implementation")
            .description("")
            .type(String.class)
            .options(RangeReaderProvider.getAvailableProviders().stream()
                    .map(RangeReaderProvider::getId)
                    .toArray())
            .group("advanced")
            .build();

    private URI uri;

    /**
     * Optional provider {@link RangeReaderProvider#getId() id}, useful to force
     * using a given provider when the URI or parameters are not enough to
     * disambiguate.
     */
    private String providerId;

    private Map<String, Object> parameterValues = new HashMap<>();

    /**
     * Creates a new, empty {@code RangeReaderConfig}.
     */
    public RangeReaderConfig() {
        // Default constructor
    }

    /**
     * Returns the URI of the resource to be read.
     *
     * @return The URI.
     */
    public URI uri() {
        return uri;
    }

    /**
     * Sets the URI of the resource to be read.
     *
     * @param uri The URI to set.
     * @return This {@code RangeReaderConfig} instance for method chaining.
     * @throws NullPointerException if the provided URI is {@code null}.
     * @throws  IllegalArgumentException If the given string violates RFC&nbsp;2396
     */
    public RangeReaderConfig uri(String uri) {
        return uri(URI.create(uri));
    }

    /**
     * Sets the URI of the resource to be read.
     *
     * @param uri The URI to set.
     * @return This {@code RangeReaderConfig} instance for method chaining.
     * @throws NullPointerException if the provided URI is {@code null}.
     */
    public RangeReaderConfig uri(URI uri) {
        this.uri = requireNonNull(uri, "uri can't be null");
        return this;
    }

    /**
     * Returns the optional provider ID.
     *
     * @return An {@link Optional} containing the provider ID, or empty if not set.
     */
    public Optional<String> providerId() {
        return Optional.ofNullable(providerId);
    }

    /**
     * Sets the optional provider ID.
     *
     * @param providerId The provider ID to set.
     * @return This {@code RangeReaderConfig} instance for method chaining.
     */
    public RangeReaderConfig providerId(String providerId) {
        this.providerId = providerId;
        return this;
    }

    /**
     * Sets a generic parameter value by its key.
     * <p>
     * {@link #providerId(String) enforcing a provider id} can also be done by calling this method
     * with {@link #FORCE_PROVIDER_ID FORCE_PROVIDER_ID.key()}
     * <p>
     * Note: This method does not validate the parameter against any known {@link RangeReaderParameter}s.
     *
     * @param key The key of the parameter.
     * @param value The value of the parameter.
     * @return This {@code RangeReaderConfig} instance for method chaining.
     */
    public RangeReaderConfig setParameter(String key, Object value) {
        if (FORCE_PROVIDER_ID.key().equals(key)) {
            this.providerId = value == null ? null : String.valueOf(value);
        }
        this.parameterValues.put(requireNonNull(key, "key"), value);
        return this;
    }

    /**
     * Sets a generic parameter value by its key.
     * <p>
     * {@link #providerId(String) enforcing a provider id} can also be done by calling this method
     * with {@link #FORCE_PROVIDER_ID}
     * <p>
     * Note: This method does not validate the parameter against any known {@link RangeReaderParameter}s.
     *
     * @param <T> the type of the parameter value
     * @param param The parameter descriptor.
     * @param value The value of the parameter.
     * @return This {@code RangeReaderConfig} instance for method chaining.
     */
    public <T> RangeReaderConfig setParameter(RangeReaderParameter<T> param, T value) {
        setParameter(param.key(), value);
        return this;
    }

    /**
     * Retrieves the value of a specific {@link RangeReaderParameter}.
     *
     * @param <T> The type of the parameter value.
     * @param param The {@link RangeReaderParameter} definition.
     * @return An {@link Optional} containing the parameter value, or empty if not set.
     */
    public <T> Optional<T> getParameter(RangeReaderParameter<T> param) {
        return getParameter(param.key(), param.type());
    }

    /**
     * Retrieves the value of a parameter by its key, returning it as an {@link Object}.
     *
     * @param key The key of the parameter.
     * @return An {@link Optional} containing the parameter value, or empty if not set.
     */
    public Optional<Object> getParameter(String key) {
        return getParameter(key, Object.class);
    }

    /**
     * Retrieves the value of a parameter by its key and attempts to convert it to the specified type.
     *
     * @param <T> The target type for the parameter value.
     * @param key The key of the parameter.
     * @param type The {@link Class} representing the target type.
     * @return An {@link Optional} containing the converted parameter value, or empty if not set.
     * @throws NullPointerException if key or type is {@code null}.
     * @throws IllegalArgumentException if the value cannot be converted to the specified type.
     */
    public <T> Optional<T> getParameter(String key, Class<T> type) {
        Object value = parameterValues.get(requireNonNull(key, "key"));
        requireNonNull(type, "type");
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(convert(value, type));
    }

    /**
     * Converts an object to a specified target type.
     * Supports conversion to {@code String}, {@code Boolean}, {@code Integer}, and {@code URI}.
     *
     * @param <T> The target type.
     * @param value The object to convert.
     * @param type The {@link Class} representing the target type.
     * @return The converted object.
     * @throws IllegalArgumentException if the conversion to the specified type is not supported.
     */
    static <T> T convert(Object value, Class<T> type) {
        if (type.isInstance(value)) return type.cast(value);

        Object converted = null;
        if (type.equals(String.class)) {
            converted = String.valueOf(value);
        } else if (type.equals(Boolean.class)) {
            converted = Boolean.valueOf(String.valueOf(value));
        } else if (type.equals(Integer.class)) {
            converted = Integer.parseInt(String.valueOf(value));
        } else if (type.equals(URI.class)) {
            converted = URI.create(String.valueOf(value));
        } else {
            throw new IllegalArgumentException("Unsupported conversion %s to %s"
                    .formatted(value.getClass().getCanonicalName(), type.getCanonicalName()));
        }
        return type.cast(converted);
    }

    /**
     * Converts this {@code RangeReaderConfig} instance into a {@link Properties} object.
     * The URI and provider ID (if set) are included, along with all other parameters.
     *
     * @return A {@link Properties} object representing this configuration.
     */
    public Properties toProperties() {
        Properties properties = new Properties();
        if (uri != null) {
            properties.setProperty(URI_KEY, uri.toString());
        }
        if (providerId != null) {
            properties.setProperty(PROVIDER_ID_KEY, providerId);
        }

        parameterValues.forEach((name, v) -> {
            if (v != null) {
                String value = String.valueOf(v);
                properties.setProperty(name, value);
            }
        });
        return properties;
    }

    /**
     * Creates a {@code RangeReaderConfig} instance from a {@link Properties} object.
     * The properties must contain the {@link #URI_KEY}.
     *
     * @param properties The {@link Properties} object to convert.
     * @return A new {@code RangeReaderConfig} instance.
     * @throws NullPointerException if properties or the URI_KEY is {@code null}.
     * @throws IllegalArgumentException if the URI_KEY is missing from the properties.
     */
    public static RangeReaderConfig fromProperties(Properties properties) {
        requireNonNull(properties);
        Object urip = requireNonNull(properties.get(URI_KEY), "Properties must include " + URI_KEY);

        URI uri = convertToURI(urip);
        String providerId = properties.getProperty(FORCE_PROVIDER_ID.key());

        RangeReaderConfig config = new RangeReaderConfig().uri(uri);
        config.providerId(providerId);

        Properties copy = new Properties();
        copy.putAll(properties);
        copy.remove(URI_KEY);
        copy.remove(PROVIDER_ID_KEY);
        copy.forEach((k, v) -> config.setParameter(String.valueOf(k), v));
        return config;
    }

    /**
     * Converts various URI-like objects to a {@link URI}.
     * Handles {@link URI}, {@link URL}, {@link Path}, {@link File}, and {@link String} types.
     *
     * @param uriObject The object to convert to a URI
     * @return A {@link URI} instance
     * @throws NullPointerException if {@code uriObject} is null
     * @throws IllegalArgumentException if the object cannot be converted to a valid URI
     */
    public static URI convertToURI(Object uriObject) {
        requireNonNull(uriObject);
        if (uriObject instanceof URI uri) {
            return uri;
        }
        if (uriObject instanceof URL url) {
            try {
                return url.toURI();
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid URL: " + url, e);
            }
        } else if (uriObject instanceof Path path) {
            return path.toUri();
        } else if (uriObject instanceof File file) {
            return file.toURI();
        }
        // Handle string representations - could be URI string or file path
        String uriString = uriObject.toString();
        try {
            URI uri = URI.create(uriString);
            // If no scheme, it might be a file path string - but only if it looks like a path
            if (uri.getScheme() == null) {
                if (looksLikeFilePath(uriString)) {
                    return Path.of(uriString).toUri();
                }
                // No scheme and doesn't look like a file path - not a valid URI
                throw new IllegalArgumentException("Invalid URI (no scheme): " + uriString
                        + ". Expected a URI with scheme (e.g., file:// or http://) or a valid file path.");
            }
            return uri;
        } catch (IllegalArgumentException e) {
            // If URI.create() fails (e.g., Windows path with backslashes), try interpreting as a file path
            if (looksLikeFilePath(uriString)) {
                try {
                    return Path.of(uriString).toUri();
                } catch (Exception pathException) {
                    throw new IllegalArgumentException("Invalid file path: " + uriString, e);
                }
            }
            throw new IllegalArgumentException("Invalid URI: " + uriString, e);
        }
    }

    /**
     * Checks if a string looks like it could be a file path.
     * Returns true if the string contains path separators or starts with path indicators.
     */
    private static boolean looksLikeFilePath(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        // Check for common path patterns
        return str.contains("/") // Unix separator
                || str.contains("\\") // Windows separator
                || str.startsWith(".") // Relative path (./file or ../file)
                || str.startsWith("~") // Home directory
                || (str.length() >= 2 && str.charAt(1) == ':'); // Windows drive letter (C:)
    }

    /**
     * Creates a {@code RangeReaderConfig} instance populated with default values from a list of parameters.
     *
     * @param parameters The list of {@link RangeReaderParameter}s from which to get default values.
     * @return A new {@code RangeReaderConfig} instance with default parameter values.
     */
    public static RangeReaderConfig withDefaults(List<RangeReaderParameter<?>> parameters) {
        RangeReaderConfig config = new RangeReaderConfig();
        parameters.stream()
                .filter(p -> p.defaultValue().isPresent())
                .forEach(p -> config.setParameter(p.key(), p.defaultValue().orElseThrow()));
        return config;
    }

    /**
     * Checks if a given {@code RangeReaderConfig} matches a specific provider ID and accepted URI schemes.
     *
     * @param config The {@code RangeReaderConfig} to check.
     * @param providerId The ID of the provider to match against.
     * @param acceptedUriSchemes An array of URI schemes that the provider accepts (e.g., "file", "http").
     *                           If {@code null}, it matches if the config URI also has a {@code null} scheme.
     * @return {@code true} if the config matches the provider ID and one of the accepted URI schemes, {@code false} otherwise.
     * @throws NullPointerException if config or providerId is {@code null}.
     */
    public static boolean matches(RangeReaderConfig config, String providerId, String... acceptedUriSchemes) {
        requireNonNull(config, "config parameter is null");
        requireNonNull(providerId, "providerId parameter is null");
        requireNonNull(config.uri(), "config uri is null");
        if (config.providerId().isPresent()
                && !config.providerId().orElseThrow().equals(providerId)) {
            return false;
        }
        // may be null
        final String scheme = config.uri().getScheme();
        return Arrays.asList(acceptedUriSchemes).contains(scheme);
    }
}
