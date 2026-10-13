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
package io.tileverse.storage.s3;

import io.tileverse.storage.s3.S3ClientCache.CredentialsSource;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NullMarked;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsProfileRegionProvider;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.InstanceProfileRegionProvider;
import software.amazon.awssdk.regions.providers.SystemSettingsRegionProvider;
import software.amazon.awssdk.utils.Lazy;

/**
 * The AWS region named by the environment of the process, for a store naming none.
 *
 * <p>The steps follow the default region chain of the AWS SDK: the {@code aws.region} system property, the
 * {@code AWS_REGION} environment variable, the AWS profile files, then EC2 metadata.
 *
 * <p>EC2 metadata is the only step waiting on the network, and outside EC2 it takes seconds to give up. It is asked
 * once per process, and only for a store on the default credentials chain: an EC2 instance role is one of its sources.
 */
@Slf4j
@NullMarked
final class EnvironmentRegion {

    /** The environment of the process, read by the providers created with the public constructor. */
    static final EnvironmentRegion INSTANCE = new EnvironmentRegion(
            new SystemSettingsRegionProvider(), EnvironmentRegion::profileFiles, new InstanceProfileRegionProvider());

    private final AwsRegionProvider systemSettings;
    private final Function<Optional<String>, AwsRegionProvider> profileFiles;
    private final Lazy<Optional<String>> ec2Metadata;

    /**
     * Reads each step from its provider; tests pass their own. A provider finding no region throws.
     *
     * @param profileFiles the provider of the region of a profile; without a name, of the profile of the environment
     */
    EnvironmentRegion(
            AwsRegionProvider systemSettings,
            Function<Optional<String>, AwsRegionProvider> profileFiles,
            AwsRegionProvider ec2Metadata) {
        this.systemSettings = systemSettings;
        this.profileFiles = profileFiles;
        this.ec2Metadata = new Lazy<>(() -> regionFrom(ec2Metadata));
    }

    /**
     * Finds the region of the environment for a store.
     *
     * @param profile the profile named by the store, if any
     * @param credentials the source of the credentials of the store
     */
    Optional<String> find(Optional<String> profile, CredentialsSource credentials) {
        Optional<String> configured = configuredRegion(profile);
        boolean mayAskEc2Metadata = credentials == CredentialsSource.DEFAULT_CHAIN;
        if (configured.isEmpty() && mayAskEc2Metadata) {
            return ec2Metadata.getValue();
        }
        return configured;
    }

    /** The region of the steps reading only the process and its files. */
    private Optional<String> configuredRegion(Optional<String> profile) {
        Optional<String> systemSetting = regionFrom(systemSettings);
        if (systemSetting.isPresent()) {
            return systemSetting;
        }
        AwsRegionProvider profileRegion = profileFiles.apply(profile);
        return regionFrom(profileRegion);
    }

    private static Optional<String> regionFrom(AwsRegionProvider provider) {
        try {
            Region region = provider.getRegion();
            return Optional.ofNullable(region).map(Region::id);
        } catch (RuntimeException noRegion) {
            log.debug("No region from {}: {}", provider.getClass().getSimpleName(), noRegion.getMessage());
            return Optional.empty();
        }
    }

    private static AwsRegionProvider profileFiles(Optional<String> profile) {
        // without a name the provider reads the profile named by AWS_PROFILE, or "default"
        String profileName = profile.orElse(null);
        return new AwsProfileRegionProvider(null, profileName);
    }
}
