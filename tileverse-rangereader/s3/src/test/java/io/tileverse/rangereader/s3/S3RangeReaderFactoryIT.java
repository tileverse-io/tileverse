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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeNoException;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.RangeReaderFactory;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Integration tests for S3RangeReaderFactory that verify URL parsing and RangeReader creation
 * using real, publicly accessible S3 resources.
 *
 * <p><strong>AWS Credentials Required:</strong> These tests require valid AWS credentials to access
 * real S3 buckets. The tests will be automatically skipped if credentials are not available.
 *
 * <h2>Setting Up AWS Credentials</h2>
 *
 * <p>AWS credentials can be provided through several methods (in order of precedence):
 *
 * <h3>1. Environment Variables</h3>
 * <pre>
 * export AWS_ACCESS_KEY_ID=your-access-key-id
 * export AWS_SECRET_ACCESS_KEY=your-secret-access-key
 * export AWS_DEFAULT_REGION=us-west-2  # optional
 * </pre>
 *
 * <h3>2. AWS Credentials File</h3>
 * <p>Create or edit {@code ~/.aws/credentials}:
 * <pre>
 * [default]
 * aws_access_key_id = your-access-key-id
 * aws_secret_access_key = your-secret-access-key
 * </pre>
 *
 * <p>And optionally {@code ~/.aws/config}:
 * <pre>
 * [default]
 * region = us-west-2
 * </pre>
 *
 * <h3>3. AWS CLI</h3>
 * <p>Run {@code aws configure} and follow the prompts:
 * <pre>
 * $ aws configure
 * AWS Access Key ID: your-access-key-id
 * AWS Secret Access Key: your-secret-access-key
 * Default region name: us-west-2
 * Default output format: json
 * </pre>
 *
 * <h3>4. IAM Roles (for EC2/ECS/Lambda)</h3>
 * <p>When running on AWS infrastructure, credentials can be automatically obtained from:
 * <ul>
 * <li>EC2 instance metadata service</li>
 * <li>ECS task role</li>
 * <li>Lambda execution role</li>
 * </ul>
 *
 * <h3>5. System Properties</h3>
 * <pre>
 * -Daws.accessKeyId=your-access-key-id
 * -Daws.secretAccessKey=your-secret-access-key
 * -Daws.region=us-west-2
 * </pre>
 *
 * <h2>Minimum Required Permissions</h2>
 *
 * <p>The AWS credentials only need read access to the public S3 buckets used in tests.
 * For the Overture Maps dataset, no special permissions are required as it's publicly accessible.
 * However, the AWS credential chain still needs to be able to authenticate.
 *
 * <p>A minimal IAM policy would be:
 * <pre>
 * {
 *   "Version": "2012-10-17",
 *   "Statement": [
 *     {
 *       "Effect": "Allow",
 *       "Action": [
 *         "s3:GetObject",
 *         "s3:HeadObject"
 *       ],
 *       "Resource": [
 *         "arn:aws:s3:::overturemaps-tiles-us-west-2-beta/*"
 *       ]
 *     }
 *   ]
 * }
 * </pre>
 *
 * <h2>Troubleshooting</h2>
 *
 * <p>If tests are being skipped, verify credentials are working:
 * <pre>
 * # Test AWS CLI access
 * aws sts get-caller-identity
 *
 * # Test S3 access to the test bucket
 * aws s3 ls s3://overturemaps-tiles-us-west-2-beta/2025-08-20/ --no-sign-request
 * </pre>
 *
 * @see <a href="https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html">AWS SDK for Java Credentials</a>
 * @see <a href="https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html">AWS CLI Configuration</a>
 */
class S3RangeReaderFactoryIT {

    @BeforeAll
    static void checkCredentialsProvider() {
        DefaultCredentialsProvider defaultCredentialsProvider =
                DefaultCredentialsProvider.builder().build();
        try {
            defaultCredentialsProvider.resolveCredentials();
        } catch (SdkClientException noCredentials) {
            assumeNoException("Test requires AWS credentials", noCredentials);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("provideRealS3Urls")
    void testRealS3Urls(String description, String url) throws IOException {
        URI s3Uri = URI.create(url);

        // Verify we can create a range reader for real URLs
        RangeReader rangeReader = RangeReaderFactory.create(s3Uri, new Properties());
        assertNotNull("Should be able to create RangeReader for " + description, rangeReader);
    }

    /**
     * Test cases using real, publicly accessible S3 URLs in different formats.
     * These test that our URL parsing works with actual S3 resources.
     */
    private static Stream<Arguments> provideRealS3Urls() {
        return Stream.of(
                // Overture Maps - different URL formats for the same object
                Arguments.of(
                        "Overture Maps virtual hosted-style (legacy format)",
                        "https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2025-08-20/base.pmtiles"),
                Arguments.of(
                        "Overture Maps virtual hosted-style with explicit region",
                        "https://overturemaps-tiles-us-west-2-beta.s3.us-west-2.amazonaws.com/2025-08-20/base.pmtiles"),
                Arguments.of(
                        "Overture Maps path-style with explicit region",
                        "https://s3.us-west-2.amazonaws.com/overturemaps-tiles-us-west-2-beta/2025-08-20/base.pmtiles"),
                Arguments.of(
                        "Overture Maps S3 URI scheme", "s3://overturemaps-tiles-us-west-2-beta/2025-08-20/base.pmtiles")

                // Add more real public datasets here as needed
                // For example, if there are other known public S3 datasets with predictable URLs
                );
    }
}
