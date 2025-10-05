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
package io.tileverse.rangereader.s3;

import static io.tileverse.rangereader.spi.RangeReaderParameter.SUBGROUP_AUTHENTICATION;
import static java.util.function.Predicate.not;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.s3.S3RangeReader.Builder;
import io.tileverse.rangereader.spi.AbstractRangeReaderProvider;
import io.tileverse.rangereader.spi.RangeReaderConfig;
import io.tileverse.rangereader.spi.RangeReaderParameter;
import io.tileverse.rangereader.spi.RangeReaderProvider;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

/**
 * {@link RangeReaderProvider} implementation for AWS S3.
 * <p>
 * The {@link RangeReaderConfig#uri() URI} is used to extract the bucket and object name from an S3 URI.
 * <p>
 * An S3 URL, or Amazon Simple Storage Service Uniform Resource Locator, refers to the
 * address used to access resources stored within AWS S3. There
 * are several forms of S3 URLs, depending on the context and desired access
 * method:
 * <h2> Path-Style URLs</h2>
 *
 * <ul>
 * <li>{@code s3://} URI: This is the canonical URI format for referencing
 * objects within S3. It is commonly used within AWS
 * services, tools, and libraries for internal referencing. For example:
 * <pre>
 * {@literal s3://your-bucket-name/your-object-name}
 * </pre>
 * <li>Public HTTP/HTTPS URLs: If an object is configured for public access, it
 * can be accessed directly via a standard HTTP or HTTPS URL. These URLs are
 * typically in the format:
 * <pre>
 * {@literal https://your-bucket-name.s3.your-aws-region.amazonaws.com/your-object-name}
 * </pre>
 * </ul>
 * When {@code http/s} URL schemes are used, {@link #canProcessHeaders(URI, Map)} disambiguates
 * by checking if a header starting with {@code x-amz-} was returned from the HEAD request.
 */
public class S3RangeReaderProvider extends AbstractRangeReaderProvider {

    private static final Logger logger = LoggerFactory.getLogger(S3RangeReaderProvider.class);

    /**
     * Key used as environment variable name to disable this range reader provider
     * <pre>
     * {@code export IO_TILEVERSE_RANGEREADER_S3=false}
     * </pre>
     */
    public static final String ENABLED_KEY = "IO_TILEVERSE_RANGEREADER_S3";
    /**
     * This range reader implementation's {@link #getId() unique identifier}
     */
    public static final String ID = "s3";

    /**
     * Creates a new S3RangeReaderProvider with support for caching parameters
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_ENABLED
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_BLOCK_ALIGNED
     * @see AbstractRangeReaderProvider#MEMORY_CACHE_BLOCK_SIZE
     */
    public S3RangeReaderProvider() {
        super(true);
    }

    /**
     * A {@link RangeReaderParameter} to enable or disable S3 path style access. When enabled, requests will use
     * path-style addressing (e.g., {@code https://s3.amazonaws.com/bucket/key}). When disabled, virtual-hosted-style
     * addressing will be used instead (e.g., {@code https://bucket.s3.amazonaws.com/key}). This can be useful for
     * compatibility with S3-compatible storage systems that do not support virtual-hosted-style requests.
     */
    public static final RangeReaderParameter<Boolean> S3_FORCE_PATH_STYLE = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.s3.force-path-style")
            .title("Enable S3 path style access")
            .description(
                    """
                When enabled, requests will use path-style addressing (e.g., https://s3.amazonaws.com/bucket/key).

                When disabled, virtual-hosted-style addressing will be used instead \
                (e.g., https://bucket.s3.amazonaws.com/key).

                This can be useful for compatibility with S3-compatible storage systems that do not \
                support virtual-hosted-style requests.

                Note: When a complete S3 URL is provided, path style is automatically detected and enabled \
                for non-AWS endpoints (MinIO, Google Cloud Storage, etc.). This parameter allows explicit \
                override of the automatic detection behavior.
                """)
            .type(Boolean.class)
            .group(ID)
            .defaultValue(true)
            .build();

    /** Configuration parameter for AWS S3 region. */
    public static final RangeReaderParameter<String> S3_REGION = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.s3.region")
            .title("Region")
            .description(
                    """
                    Configure the region with which the SDK should communicate.

                    If this is not specified, the SDK will attempt to identify the endpoint automatically using the following logic:

                    * Check the 'aws.region' system property for the region.
                    * Check the 'AWS_REGION' environment variable for the region.
                    * Check the {user.home}/.aws/credentials and {user.home}/.aws/config files for the region.
                    * If running in EC2, check the EC2 metadata service for the region.

                    If the region is not found, an exception will be thrown.

                    Each AWS region corresponds to a separate geographical location where a set of Amazon services is deployed. These \
                    regions (except for the special `aws-global` and `aws-cn-global` regions) are separate from each other, \
                    with their own set of resources. This means a resource created in one region (eg. an SQS queue) is not available in \
                    another region.
                    """)
            .type(String.class)
            .group(ID)
            // filter out global regions as of Region.of(String)
            .options(Region.regions().stream()
                    .filter(not(Region::isGlobalRegion))
                    .map(Region::id)
                    .toArray())
            .build();

    /**
     * The AWS access key ID to use for authentication when both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are provided.
     */
    public static final RangeReaderParameter<String> S3_AWS_ACCESS_KEY_ID = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.s3.aws-access-key-id")
            .title("AWS Access Key ID")
            .description(
                    """
                    The AWS access key ID to use for authentication.

                    This parameter must be used together with AWS_SECRET_ACCESS_KEY. When both are provided, \
                    they will be used for authentication regardless of the USE_DEFAULT_CREDENTIALS_PROVIDER setting.

                    If neither AWS_ACCESS_KEY_ID nor AWS_SECRET_ACCESS_KEY are provided, authentication behavior \
                    is controlled by the USE_DEFAULT_CREDENTIALS_PROVIDER parameter.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /**
     * The AWS secret access key to use for authentication when both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are provided.
     */
    public static final RangeReaderParameter<String> S3_AWS_SECRET_ACCESS_KEY = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.s3.aws-secret-access-key")
            .title("AWS Secret Access Key")
            .description(
                    """
                    The AWS secret access key to use for authentication.

                    This parameter must be used together with AWS_ACCESS_KEY_ID. When both are provided, \
                    they will be used for authentication regardless of the USE_DEFAULT_CREDENTIALS_PROVIDER setting.

                    If neither AWS_ACCESS_KEY_ID nor AWS_SECRET_ACCESS_KEY are provided, authentication behavior \
                    is controlled by the USE_DEFAULT_CREDENTIALS_PROVIDER parameter.
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    /** Configuration parameter to control whether to use the default AWS credentials provider chain. */
    public static final RangeReaderParameter<Boolean> S3_USE_DEFAULT_CREDENTIALS_PROVIDER =
            RangeReaderParameter.builder()
                    .key("io.tileverse.rangereader.s3.use-default-credentials-provider")
                    .title("Use Default Credentials Provider")
                    .description(
                            """
                    When enabled, the AWS default credentials provider chain is used, which looks for credentials \
                    in this order:
                      1. Java System Properties - aws.accessKeyId and aws.secretAccessKey
                      2. Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
                      3. Web Identity Token File - from the path specified in the AWS_WEB_IDENTITY_TOKEN_FILE environment variable
                      4. Shared Credentials File - at ~/.aws/credentials
                      5. Amazon ECS Container Credentials - loaded from the endpoint specified in the AWS_CONTAINER_CREDENTIALS_RELATIVE_URI environment variable
                      6. Amazon EC2 Instance Profile Credentials - loaded from the Amazon EC2 metadata service

                    If neither default credentials provider or access/secret key are used, annonymous access will \
                    be attempted.
                    """)
                    .type(Boolean.class)
                    .group(ID)
                    .subgroup(SUBGROUP_AUTHENTICATION)
                    .build();

    /** Configuration parameter to specify a custom AWS credentials profile name. */
    public static final RangeReaderParameter<String> S3_DEFAULT_CREDENTIALS_PROFILE = RangeReaderParameter.builder()
            .key("io.tileverse.rangereader.s3.default-credentials-profile")
            .title("Default Credentials Profile")
            .description(
                    """
                    The AWS credentials profile name to use when USE_DEFAULT_CREDENTIALS_PROVIDER is enabled.

                    If not specified, the 'default' profile is used. This parameter is only effective when \
                    USE_DEFAULT_CREDENTIALS_PROVIDER is set to true.

                    The profile should exist in the AWS credentials file (typically ~/.aws/credentials) or \
                    AWS config file (typically ~/.aws/config).
                    """)
            .type(String.class)
            .group(ID)
            .subgroup(SUBGROUP_AUTHENTICATION)
            .build();

    static final List<RangeReaderParameter<?>> PARAMS = List.of(
            S3_FORCE_PATH_STYLE,
            S3_REGION,
            S3_AWS_ACCESS_KEY_ID,
            S3_AWS_SECRET_ACCESS_KEY,
            S3_USE_DEFAULT_CREDENTIALS_PROVIDER,
            S3_DEFAULT_CREDENTIALS_PROFILE);

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public boolean isAvailable() {
        return RangeReaderProvider.isEnabled(ENABLED_KEY);
    }

    @Override
    public String getDescription() {
        return "AWS S3 Range Reader";
    }

    @Override
    public int getOrder() {
        return -100; // High priority
    }

    @Override
    protected List<RangeReaderParameter<?>> buildParameters() {
        return PARAMS;
    }

    @Override
    protected RangeReader createInternal(RangeReaderConfig opts) throws IOException {
        Builder builder = prepareRangeReaderBuilder(opts);
        return builder.build();
    }

    Builder prepareRangeReaderBuilder(RangeReaderConfig opts) {
        URI uri = opts.uri();
        Builder builder = S3RangeReader.builder().uri(uri);
        opts.getParameter(S3_FORCE_PATH_STYLE).ifPresent(builder::forcePathStyle);
        opts.getParameter(S3_REGION).filter(r -> !r.isBlank()).map(Region::of).ifPresent(builder::region);
        opts.getParameter(S3_AWS_ACCESS_KEY_ID).ifPresent(builder::awsAccessKeyId);
        opts.getParameter(S3_AWS_SECRET_ACCESS_KEY).ifPresent(builder::awsSecretAccessKey);
        opts.getParameter(S3_USE_DEFAULT_CREDENTIALS_PROVIDER).ifPresent(builder::useDefaultCredentialsProvider);
        opts.getParameter(S3_DEFAULT_CREDENTIALS_PROFILE).ifPresent(builder::defaultCredentialsProfile);
        return builder;
    }

    @Override
    public boolean canProcess(RangeReaderConfig config) {
        if (RangeReaderConfig.matches(config, getId(), "s3", "http", "https")) {
            try {
                S3Reference l = S3CompatibleUrlParser.parseS3Url(config.uri());

                // RangeReader requires both bucket AND key to read a specific file
                // This ensures ambiguous URLs (like custom domains) fall back to HTTP
                boolean hasValidBucket =
                        l.bucket() != null && !l.bucket().trim().isEmpty();
                boolean hasValidKey = l.key() != null && !l.key().trim().isEmpty();

                if (!hasValidBucket || !hasValidKey) {
                    logger.debug("Skipping URL {} - bucket='{}', key='{}'", config.uri(), l.bucket(), l.key());
                    return false;
                }

                return true;
            } catch (IllegalArgumentException e) {
                logger.debug("Can't process URL {}: {}", config.uri(), e.getMessage());
            }
        }
        return false;
    }

    @Override
    public boolean canProcessHeaders(URI uri, Map<String, List<String>> headers) {
        Set<String> headerNames = headers.keySet();
        return headerNames.stream().anyMatch("x-amz-request-id"::equalsIgnoreCase);
    }
}
