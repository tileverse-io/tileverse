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
package io.tileverse.rangereader.http;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HTTP Digest Authentication implementation for HttpRangeReader.
 * <p>
 * This authenticator implements the Digest Access Authentication as specified in RFC 2617.
 * It manages the complexities of digest authentication including nonce management,
 * challenge-response flows, and algorithm selection.
 * <p>
 * Note: This implementation does not support the full spectrum of digest authentication
 * features but covers the most commonly used configurations.
 */
public class DigestAuthentication implements HttpAuthentication {

    private static final Logger LOGGER = Logger.getLogger(DigestAuthentication.class.getName());
    private static final String HEX_CHARS = "0123456789abcdef";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final String username;
    private final String password;
    private final Map<URI, DigestParams> digestParamsCache = new ConcurrentHashMap<>();
    private final AtomicInteger nonceCounter = new AtomicInteger(1);

    /**
     * Creates a new Digest Authentication instance.
     *
     * @param username The username
     * @param password The password
     */
    public DigestAuthentication(String username, String password) {
        this.username = Objects.requireNonNull(username, "Username cannot be null");
        this.password = Objects.requireNonNull(password, "Password cannot be null");
    }

    @Override
    public HttpRequest.Builder authenticate(HttpClient httpClient, HttpRequest.Builder requestBuilder) {
        URI uri = requestBuilder.build().uri();

        // Try to get cached digest parameters for this URI
        DigestParams params = digestParamsCache.get(uri);

        // If we don't have digest params yet, make a pre-flight request to get them
        if (params == null) {
            try {
                params = fetchDigestParams(httpClient, uri);
                if (params != null) {
                    digestParamsCache.put(uri, params);
                } else {
                    // If we couldn't get digest params, return without authentication
                    LOGGER.warning("Could not get digest authentication parameters for " + uri);
                    return requestBuilder;
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to fetch digest authentication parameters", e);
                return requestBuilder;
            }
        }

        // Generate the digest response
        String digestHeader = generateDigestHeader(uri, "GET", params);

        // Add the Authorization header
        return requestBuilder.header("Authorization", digestHeader);
    }

    /**
     * Fetches digest authentication parameters by making a pre-flight request
     * and parsing the WWW-Authenticate header from the 401 response.
     *
     * @param uri The URI to authenticate against
     * @return The digest parameters, or null if digest auth is not supported
     */
    private DigestParams fetchDigestParams(HttpClient httpClient, URI uri) {
        try {
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .method("HEAD", HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

            // We expect a 401 Unauthorized response with a WWW-Authenticate header
            if (response.statusCode() == 401) {
                return parseWwwAuthenticateHeader(response, uri);
            } else {
                LOGGER.warning("Expected 401 response for digest auth, got " + response.statusCode());
                return null;
            }

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error during pre-flight request for digest auth", e);
            return null;
        }
    }

    /**
     * Parses the WWW-Authenticate header from a 401 response to extract digest parameters.
     *
     * @param response The HTTP response containing the WWW-Authenticate header
     * @param uri The URI being authenticated
     * @return The digest parameters, or null if digest auth is not supported
     */
    private DigestParams parseWwwAuthenticateHeader(HttpResponse<Void> response, URI uri) {
        return response.headers().allValues("WWW-Authenticate").stream()
                .filter(h -> h.toLowerCase().startsWith("digest "))
                .findFirst()
                .map(h -> {
                    Map<String, String> params = new HashMap<>();

                    // Extract key-value pairs from the header
                    // Format is typically: Digest key1="value1", key2="value2", ...
                    Pattern pattern = Pattern.compile("(\\w+)=(?:\"([^\"]*)\"|([^,]*))");
                    Matcher matcher = pattern.matcher(h.substring(7)); // Skip "Digest "

                    while (matcher.find()) {
                        String key = matcher.group(1);
                        String value = matcher.group(2) != null ? matcher.group(2) : matcher.group(3);
                        params.put(key.toLowerCase(), value);
                    }

                    // Create DigestParams object
                    String realm = params.getOrDefault("realm", "");
                    String nonce = params.getOrDefault("nonce", "");
                    String opaque = params.getOrDefault("opaque", "");
                    String algorithm = params.getOrDefault("algorithm", "MD5");
                    boolean qop = params.containsKey("qop");
                    String qopValue = params.getOrDefault("qop", "");

                    return new DigestParams(realm, nonce, opaque, algorithm, qop, qopValue, uri.getPath());
                })
                .orElse(null);
    }

    /**
     * Generates a digest authorization header value.
     *
     * @param uri The URI being authenticated
     * @param method The HTTP method (GET, HEAD, etc.)
     * @param params The digest parameters
     * @return The complete digest authorization header value
     */
    private String generateDigestHeader(URI uri, String method, DigestParams params) {
        // Generate a client nonce if QoP requires it
        String cnonce = "";
        String nc = "";

        if (params.qop) {
            cnonce = generateCnonce();
            nc = String.format("%08x", nonceCounter.getAndIncrement());
        }

        // Calculate digest response
        String ha1 = calculateHA1(params.algorithm, username, password, params.realm);
        String ha2 = calculateHA2(method, uri.getPath());
        String response;

        if (params.qop) {
            // If QoP is specified, include cnonce and nc in the calculation
            response = calculateResponseWithQop(ha1, params.nonce, nc, cnonce, params.qopValue, ha2);
        } else {
            // Simple calculation without QoP
            response = calculateResponse(ha1, params.nonce, ha2);
        }

        // Build the complete Authorization header
        StringBuilder headerValue = new StringBuilder();
        headerValue
                .append("Digest username=\"")
                .append(username)
                .append("\", ")
                .append("realm=\"")
                .append(params.realm)
                .append("\", ")
                .append("nonce=\"")
                .append(params.nonce)
                .append("\", ")
                .append("uri=\"")
                .append(params.uri)
                .append("\", ")
                .append("response=\"")
                .append(response)
                .append("\"");

        if (params.algorithm != null && !params.algorithm.isEmpty() && !params.algorithm.equalsIgnoreCase("MD5")) {
            headerValue.append(", algorithm=").append(params.algorithm);
        }

        if (params.opaque != null && !params.opaque.isEmpty()) {
            headerValue.append(", opaque=\"").append(params.opaque).append("\"");
        }

        if (params.qop) {
            headerValue
                    .append(", qop=")
                    .append(params.qopValue)
                    .append(", nc=")
                    .append(nc)
                    .append(", cnonce=\"")
                    .append(cnonce)
                    .append("\"");
        }

        return headerValue.toString();
    }

    /**
     * Calculates the HA1 part of the digest response.
     *
     * @param algorithm The digest algorithm (MD5 or MD5-sess)
     * @param username The username
     * @param password The password
     * @param realm The authentication realm
     * @return The HA1 hash value
     */
    private String calculateHA1(String algorithm, String username, String password, String realm) {
        String ha1 = md5(username + ":" + realm + ":" + password);

        if (algorithm != null && algorithm.equalsIgnoreCase("MD5-sess")) {
            // For MD5-sess, HA1 = MD5(MD5(username:realm:password):nonce:cnonce)
            String cnonce = generateCnonce();
            ha1 = md5(ha1 + ":" + cnonce);
        }

        return ha1;
    }

    /**
     * Calculates the HA2 part of the digest response.
     *
     * @param method The HTTP method
     * @param uri The request URI
     * @return The HA2 hash value
     */
    private String calculateHA2(String method, String uri) {
        return md5(method + ":" + uri);
    }

    /**
     * Calculates the digest response without QoP.
     *
     * @param ha1 The HA1 value
     * @param nonce The server nonce
     * @param ha2 The HA2 value
     * @return The digest response
     */
    private String calculateResponse(String ha1, String nonce, String ha2) {
        return md5(ha1 + ":" + nonce + ":" + ha2);
    }

    /**
     * Calculates the digest response with QoP.
     *
     * @param ha1 The HA1 value
     * @param nonce The server nonce
     * @param nc The nonce count
     * @param cnonce The client nonce
     * @param qop The quality of protection value
     * @param ha2 The HA2 value
     * @return The digest response
     */
    private String calculateResponseWithQop(
            String ha1, String nonce, String nc, String cnonce, String qop, String ha2) {
        return md5(ha1 + ":" + nonce + ":" + nc + ":" + cnonce + ":" + qop + ":" + ha2);
    }

    /**
     * Generates a random client nonce.
     *
     * @return A random client nonce value
     */
    private String generateCnonce() {
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);
        return toHexString(bytes);
    }

    /**
     * Computes an MD5 hash of the input string.
     *
     * @param input The input string to hash
     * @return The MD5 hash as a hex string
     */
    private String md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes());
            return toHexString(digest);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.log(Level.SEVERE, "MD5 algorithm not available", e);
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    /**
     * Converts a byte array to a hex string.
     *
     * @param bytes The byte array to convert
     * @return The hex string representation
     */
    private String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder(2 * bytes.length);
        for (byte b : bytes) {
            sb.append(HEX_CHARS.charAt((b & 0xF0) >> 4)).append(HEX_CHARS.charAt(b & 0x0F));
        }
        return sb.toString();
    }

    /**
     * Container class for digest authentication parameters.
     */
    private static class DigestParams {
        final String realm;
        final String nonce;
        final String opaque;
        final String algorithm;
        final boolean qop;
        final String qopValue;
        final String uri;

        public DigestParams(
                String realm, String nonce, String opaque, String algorithm, boolean qop, String qopValue, String uri) {
            this.realm = realm;
            this.nonce = nonce;
            this.opaque = opaque;
            this.algorithm = algorithm;
            this.qop = qop;
            this.qopValue = qopValue;
            this.uri = uri;
        }
    }
}
