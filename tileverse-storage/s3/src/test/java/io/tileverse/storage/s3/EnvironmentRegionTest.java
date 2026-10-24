/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import static io.tileverse.storage.s3.S3ClientCache.CredentialsSource.ACCESS_KEYS;
import static io.tileverse.storage.s3.S3ClientCache.CredentialsSource.ANONYMOUS;
import static io.tileverse.storage.s3.S3ClientCache.CredentialsSource.DEFAULT_CHAIN;
import static io.tileverse.storage.s3.S3ClientCache.CredentialsSource.NAMED_PROFILE;
import static org.assertj.core.api.Assertions.assertThat;

import io.tileverse.storage.s3.S3ClientCache.CredentialsSource;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;

class EnvironmentRegionTest {

    private static final Optional<String> NO_PROFILE = Optional.empty();
    private static final Optional<String> ANALYTICS = Optional.of("analytics");
    private static final Map<String, String> NO_PROFILE_REGIONS = Map.of();

    @ParameterizedTest(name = "{0}")
    @MethodSource("lookups")
    void lookupOrder(
            String scenario,
            EnvironmentRegion environment,
            Optional<String> profile,
            CredentialsSource credentials,
            Optional<String> expected) {
        assertThat(environment.find(profile, credentials)).as(scenario).isEqualTo(expected);
    }

    static Stream<Arguments> lookups() {
        Map<String, String> defaultProfile = Map.of("default", "eu-west-2");
        Map<String, String> bothProfiles = Map.of("default", "eu-west-2", "analytics", "ap-south-1");
        return Stream.of(
                Arguments.of(
                        "system setting, profile and EC2 metadata",
                        environment("eu-west-1", defaultProfile, "eu-west-3"),
                        NO_PROFILE,
                        DEFAULT_CHAIN,
                        Optional.of("eu-west-1")),
                Arguments.of(
                        "profile and EC2 metadata",
                        environment(null, defaultProfile, "eu-west-3"),
                        NO_PROFILE,
                        DEFAULT_CHAIN,
                        Optional.of("eu-west-2")),
                Arguments.of(
                        "EC2 metadata alone",
                        environment(null, NO_PROFILE_REGIONS, "eu-west-3"),
                        NO_PROFILE,
                        DEFAULT_CHAIN,
                        Optional.of("eu-west-3")),
                Arguments.of(
                        "nothing",
                        environment(null, NO_PROFILE_REGIONS, null),
                        NO_PROFILE,
                        DEFAULT_CHAIN,
                        Optional.empty()),
                Arguments.of(
                        "system setting and the profile of the store",
                        environment("eu-west-1", bothProfiles, null),
                        ANALYTICS,
                        NAMED_PROFILE,
                        Optional.of("eu-west-1")),
                Arguments.of(
                        "the profile of the store",
                        environment(null, bothProfiles, null),
                        ANALYTICS,
                        NAMED_PROFILE,
                        Optional.of("ap-south-1")),
                Arguments.of(
                        "the profile of the store has no region",
                        environment(null, defaultProfile, null),
                        ANALYTICS,
                        NAMED_PROFILE,
                        Optional.empty()),
                Arguments.of(
                        "anonymous store, default profile",
                        environment(null, defaultProfile, null),
                        NO_PROFILE,
                        ANONYMOUS,
                        Optional.of("eu-west-2")),
                Arguments.of(
                        "store with access keys, default profile",
                        environment(null, defaultProfile, null),
                        NO_PROFILE,
                        ACCESS_KEYS,
                        Optional.of("eu-west-2")));
    }

    // Outside EC2 the metadata service takes seconds to give up: only a store on the default chain may wait for it.
    @ParameterizedTest
    @EnumSource(
            value = CredentialsSource.class,
            names = {"ANONYMOUS", "ACCESS_KEYS", "NAMED_PROFILE"})
    void ec2MetadataNotAsked(CredentialsSource credentials) {
        CountingProvider ec2Metadata = new CountingProvider("eu-west-3");
        EnvironmentRegion environment = withEc2Metadata(null, NO_PROFILE_REGIONS, ec2Metadata);

        Optional<String> region = environment.find(NO_PROFILE, credentials);

        assertThat(region).isEmpty();
        assertThat(ec2Metadata.asked).hasValue(0);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("earlierAnswers")
    void ec2MetadataNotAskedAfterAnAnswer(String scenario, String systemSetting, Map<String, String> profileRegions) {
        CountingProvider ec2Metadata = new CountingProvider("eu-west-3");
        EnvironmentRegion environment = withEc2Metadata(systemSetting, profileRegions, ec2Metadata);

        Optional<String> region = environment.find(NO_PROFILE, DEFAULT_CHAIN);

        assertThat(region).as(scenario).hasValue("eu-west-1");
        assertThat(ec2Metadata.asked).hasValue(0);
    }

    static Stream<Arguments> earlierAnswers() {
        return Stream.of(
                Arguments.of("system setting", "eu-west-1", NO_PROFILE_REGIONS),
                Arguments.of("profile", null, Map.of("default", "eu-west-1")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ec2MetadataAnswers")
    void ec2MetadataAskedOnce(String scenario, CountingProvider ec2Metadata, Optional<String> expected) {
        EnvironmentRegion environment = withEc2Metadata(null, NO_PROFILE_REGIONS, ec2Metadata);

        Optional<String> first = environment.find(NO_PROFILE, DEFAULT_CHAIN);
        Optional<String> second = environment.find(NO_PROFILE, DEFAULT_CHAIN);

        assertThat(first).as(scenario).isEqualTo(expected);
        assertThat(second).as(scenario).isEqualTo(expected);
        assertThat(ec2Metadata.asked).hasValue(1);
    }

    static Stream<Arguments> ec2MetadataAnswers() {
        return Stream.of(
                Arguments.of("on EC2", new CountingProvider("eu-west-3"), Optional.of("eu-west-3")),
                Arguments.of("outside EC2", new CountingProvider(null), Optional.empty()));
    }

    /** An environment answering with the given regions; {@code null} and a missing profile stand for no answer. */
    static EnvironmentRegion environment(String systemSetting, Map<String, String> profileRegions, String ec2Metadata) {
        return withEc2Metadata(systemSetting, profileRegions, answering(ec2Metadata));
    }

    private static EnvironmentRegion withEc2Metadata(
            String systemSetting, Map<String, String> profileRegions, AwsRegionProvider ec2Metadata) {
        return new EnvironmentRegion(
                answering(systemSetting),
                profile -> answering(profileRegions.get(profile.orElse("default"))),
                ec2Metadata);
    }

    /** A step answering {@code region}, or failing like the providers of the SDK when it is {@code null}. */
    private static AwsRegionProvider answering(String region) {
        return () -> {
            if (region == null) {
                throw SdkClientException.create("no region");
            }
            return Region.of(region);
        };
    }

    private static final class CountingProvider implements AwsRegionProvider {

        final AtomicInteger asked = new AtomicInteger();
        private final AwsRegionProvider answer;

        CountingProvider(String region) {
            this.answer = answering(region);
        }

        @Override
        public Region getRegion() {
            asked.incrementAndGet();
            return answer.getRegion();
        }
    }
}
