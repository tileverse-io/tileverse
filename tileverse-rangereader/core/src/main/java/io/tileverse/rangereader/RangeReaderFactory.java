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
package io.tileverse.rangereader;

import static java.util.Objects.requireNonNull;

import io.tileverse.rangereader.http.HttpRangeReaderProvider;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory for creating {@link RangeReader} instances.
 * This factory uses the Java Service Provider Interface (SPI) to discover
 * available {@link RangeReaderProvider} implementations at runtime.
 */
public final class RangeReaderFactory {
    private static final Logger logger = LoggerFactory.getLogger(RangeReaderFactory.class);

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private RangeReaderFactory() {
        // Private constructor to prevent instantiation of this utility class.
    }

    /**
     * Creates a {@link RangeReader} for the given URI.
     *
     * @param uri The URI of the resource to read.
     * @return A new {@link RangeReader} instance.
     * @throws IOException If an I/O error occurs during RangeReader creation.
     * @throws IllegalStateException If no suitable provider is found or if there is an unresolvable ambiguity.
     */
    public static RangeReader create(URI uri) throws IOException {
        return create(uri, new Properties());
    }

    /**
     * Creates a {@link RangeReader} for the given URI and configuration properties.
     * This method converts the {@link Properties} into a {@link RangeReaderConfig} and
     * then delegates to {@link #create(RangeReaderConfig)}.
     *
     * @param uri The URI of the resource to read.
     * @param config Additional configuration properties for the RangeReader.
     * @return A new {@link RangeReader} instance.
     * @throws IOException If an I/O error occurs during RangeReader creation.
     * @throws IllegalStateException If no suitable provider is found or if there is an unresolvable ambiguity.
     */
    public static RangeReader create(URI uri, Properties config) throws IOException {
        Properties properties = new Properties();
        properties.putAll(requireNonNull(config));
        properties.put(RangeReaderConfig.URI_KEY, requireNonNull(uri));
        return create(properties);
    }

    /**
     * Creates a {@link RangeReader} based on the provided configuration properties.
     * This method converts the {@link Properties} into a {@link RangeReaderConfig} and
     * then delegates to {@link #create(RangeReaderConfig)}.
     *
     * @param config A {@link Properties} object containing configuration, including the URI.
     * @return A new {@link RangeReader} instance.
     * @throws IOException If an I/O error occurs during RangeReader creation.
     * @throws IllegalStateException If no suitable provider is found or if there is an unresolvable ambiguity.
     */
    public static RangeReader create(Properties config) throws IOException {
        RangeReaderConfig fromProperties = RangeReaderConfig.fromProperties(requireNonNull(config));
        return create(fromProperties);
    }

    /**
     * Creates a {@link RangeReader} using the best available provider for the given configuration.
     * The selection process is as follows:
     * <ol>
     *   <li>If a provider ID is explicitly set in the config (via {@link RangeReaderConfig#providerId()}),
     *       only that specific provider is considered.</li>
     *   <li>For non-http(s) schemes (e.g., 'file', 's3', 'gs'), the factory attempts to find a unique
     *       {@link RangeReaderProvider} that {@link RangeReaderProvider#canProcess(RangeReaderConfig) can process}
     *       the URI's scheme.</li>
     *   <li>For http(s) schemes, a multi-step disambiguation is performed:
     *     <ol>
     *       <li>First, it checks for unique matches based on known cloud provider hostname patterns
     *           (e.g., ".blob.core.windows.net" for Azure, ".s3.amazonaws.com" for S3).</li>
     *       <li>If ambiguity remains among multiple providers, it performs a HEAD request to the URI
     *           to check for provider-specific response headers (e.g., "x-ms-request-id" for Azure,
     *           "x-amz-request-id" for S3).</li>
     *       <li>If ambiguity still remains after header inspection, it uses the provider with the
     *           highest priority (lowest value returned by {@link RangeReaderProvider#getOrder()}).</li>
     *     </ol>
     *   </li>
     *   <li>If no suitable provider is found or if an unresolvable tie-breaker is needed (multiple
     *       providers with the same highest priority), an {@link IllegalStateException} is thrown.</li>
     * </ol>
     *
     * @param config The configuration for the RangeReader, including the URI and optional provider ID.
     * @return A new {@link RangeReader} instance.
     * @throws IOException If an I/O error occurs during RangeReader creation or header probing.
     * @throws IllegalStateException If no suitable provider is found or if there is an unresolvable ambiguity.
     */
    public static RangeReader create(RangeReaderConfig config) throws IOException {
        RangeReaderProvider provider = findBestProvider(requireNonNull(config));
        return provider.create(config);
    }

    /**
     * Finds the best {@link RangeReaderProvider} for the given configuration.
     * This method encapsulates the provider selection and disambiguation logic.
     *
     * @param config The configuration for the RangeReader.
     * @return The selected {@link RangeReaderProvider}.
     * @throws IllegalStateException If no suitable provider is found or if there is an unresolvable ambiguity.
     */
    public static RangeReaderProvider findBestProvider(RangeReaderConfig config) {

        final URI uri = requireNonNull(config.uri());

        // Explicit Provider ID is the ultimate override.
        if (config.providerId().isPresent()) {
            return findByProviderId(config.providerId().orElseThrow());
        }

        final List<RangeReaderProvider> candidates = findCandidates(config);

        return switch (candidates.size()) {
            case 0 -> throw new IllegalStateException("No suitable provider found for URI: " + uri);
            case 1 -> candidates.get(0); // Unambiguous match
            default -> disambiguate(uri, candidates);
        };
    }

    /**
     * Disambiguates between multiple candidate {@link RangeReaderProvider}s.
     * For HTTP/HTTPS URIs, it performs further checks (hostname patterns, HEAD requests).
     * For other schemes, it resolves by provider priority.
     *
     * @param uri The URI of the resource.
     * @param candidates The list of candidate {@link RangeReaderProvider}s.
     * @return The best {@link RangeReaderProvider} from the candidates.
     * @throws IllegalStateException If an unresolvable ambiguity exists.
     */
    private static RangeReaderProvider disambiguate(URI uri, List<RangeReaderProvider> candidates) {
        final String scheme = uri.getScheme();
        final boolean isHttp = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
        if (isHttp) {
            // Ambiguous http(s):// scheme disambiguation.
            return disambiguateHttpUri(uri, candidates);
        }
        // Ambiguous non-http scheme. Resolve by priority.
        return resolveByPriority(candidates);
    }

    /**
     * Finds a {@link RangeReaderProvider} by its explicit ID.
     *
     * @param providerId The ID of the provider to find.
     * @return The {@link RangeReaderProvider} with the given ID.
     * @throws IllegalStateException If the provider with the specified ID is not found or not available.
     */
    private static RangeReaderProvider findByProviderId(String providerId) {
        boolean available = true; // Always check availability when explicitly requested
        return RangeReaderProvider.getProvider(providerId, available);
    }

    /**
     * Finds all {@link RangeReaderProvider} candidates that can process the given configuration.
     *
     * @param config The configuration to check against.
     * @return A list of {@link RangeReaderProvider}s that can process the config.
     */
    private static List<RangeReaderProvider> findCandidates(RangeReaderConfig config) {
        List<RangeReaderProvider> providers = RangeReaderProvider.getAvailableProviders();
        List<RangeReaderProvider> candidates =
                providers.stream().filter(p -> p.canProcess(config)).toList();
        return candidates;
    }

    /**
     * Disambiguates between multiple HTTP/HTTPS {@link RangeReaderProvider} candidates.
     * This involves checking for specific cloud provider hostname patterns and, if necessary,
     * performing a HEAD request to inspect response headers.
     *
     * @param uri The HTTP/HTTPS URI.
     * @param httpCandidates The list of candidate {@link RangeReaderProvider}s for HTTP/HTTPS.
     * @return The best {@link RangeReaderProvider} from the candidates.
     * @throws IllegalStateException If an unresolvable ambiguity exists.
     */
    private static RangeReaderProvider disambiguateHttpUri(URI uri, List<RangeReaderProvider> httpCandidates) {

        // Filter out the generic HttpRangeReaderProvider first, as specific cloud providers are preferred.
        List<RangeReaderProvider> specificCandidates = httpCandidates.stream()
                .filter(p -> !(p instanceof HttpRangeReaderProvider))
                .toList();

        try {
            // Perform a HEAD request to get headers for further disambiguation.
            Map<String, List<String>> headers = probeUriHeaders(uri);
            List<RangeReaderProvider> probedCandidates = specificCandidates.stream()
                    .filter(p -> p.canProcessHeaders(uri, headers))
                    .toList();

            if (probedCandidates.isEmpty()) {
                // If no specific cloud providers match, fall back to the generic HTTP provider.
                return httpCandidates.stream()
                        .filter(HttpRangeReaderProvider.class::isInstance)
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("HttpRangeReaderProvider not found"));
            }

            if (probedCandidates.size() == 1) {
                return probedCandidates.get(0);
            }
            // If probing narrowed down candidates, use them for priority resolution.
            specificCandidates = probedCandidates;

        } catch (Exception e) {
            logger.warn("Warning: HEAD request probe failed for " + uri + ": " + e.getMessage(), e);
        }

        // Multiple candidates still matching, find the highest priority.
        return resolveByPriority(specificCandidates);
    }

    /**
     * Resolves ambiguity among multiple {@link RangeReaderProvider}s by selecting the one
     * with the highest priority (lowest {@link RangeReaderProvider#getOrder()}).
     *
     * @param specificCandidates The list of candidate {@link RangeReaderProvider}s.
     * @return The {@link RangeReaderProvider} with the highest priority.
     * @throws IllegalStateException If multiple providers have the same highest priority, leading to an unresolvable ambiguity.
     */
    private static RangeReaderProvider resolveByPriority(List<RangeReaderProvider> specificCandidates) {

        final int highestPriority = specificCandidates.stream()
                .mapToInt(RangeReaderProvider::getOrder)
                .min()
                .orElseThrow(() -> new IllegalStateException("No candidates to resolve by priority."));
        List<RangeReaderProvider> bestCandidates = specificCandidates.stream()
                .filter(p -> p.getOrder() == highestPriority)
                .toList();

        if (bestCandidates.size() > 1) {
            String conflictingIds =
                    bestCandidates.stream().map(RangeReaderProvider::getId).collect(Collectors.joining(", "));
            throw new IllegalStateException(
                    "URI ambiguity detected. Multiple providers matched with the same priority ("
                            + highestPriority + "): [" + conflictingIds + "]. "
                            + "Please specify a provider ID in the RangeReaderConfig to resolve this ambiguity.");
        }
        return bestCandidates.get(0);
    }

    /**
     * Performs an HTTP HEAD request to the given URI and returns the response headers.
     * This is used for disambiguating HTTP(S) {@link RangeReaderProvider}s by inspecting
     * provider-specific headers.
     *
     * @param uri The URI to probe.
     * @return A map of response headers.
     * @throws IOException If an I/O error occurs during the HEAD request.
     * @throws InterruptedException If the operation is interrupted.
     */
    private static Map<String, List<String>> probeUriHeaders(URI uri) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .timeout(Duration.ofSeconds(3))
                .build();
        // This call returns a response for any status code (200, 403, 404, etc.) and throws IOException for network
        // failures.
        HttpResponse<Void> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
        return response.headers().map();
    }
}
