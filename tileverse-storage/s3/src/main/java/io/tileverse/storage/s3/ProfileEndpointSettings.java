/*
 * Copyright (c) 2026 Multiversio LLC.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.tileverse.storage.s3;

import java.util.function.Supplier;
import org.jspecify.annotations.NullMarked;
import software.amazon.awssdk.awscore.endpoint.DualstackEnabledProvider;
import software.amazon.awssdk.awscore.endpoint.FipsEnabledProvider;
import software.amazon.awssdk.profiles.ProfileFile;

/**
 * The FIPS and dualstack settings of the clients of a store naming a profile, for the presigner of that store.
 *
 * <p>The presigner of the SDK takes no profile name. A setting left unset makes it read the profile named by
 * {@code AWS_PROFILE}, or {@code default}. A presigned URL must name the host reached by the clients of the store, and
 * both settings always get a value here.
 *
 * <p>This class makes the same SDK calls as the client builder, to get the same answer without building a client. The
 * SDK marks these calls as protected API, meant for its generated clients, and a minor release may change them.
 * {@code ProfileEndpointSettingsTest} compares both answers to catch such a change at an SDK upgrade.
 *
 * @param fips whether requests go to a FIPS endpoint
 * @param dualstack whether requests go to a dualstack endpoint
 */
@NullMarked
record ProfileEndpointSettings(boolean fips, boolean dualstack) {

    /** The settings found by the S3 client builder in the process and in {@code profile} of {@code profileFile}. */
    static ProfileEndpointSettings resolve(Supplier<ProfileFile> profileFile, String profile) {
        FipsEnabledProvider fips = FipsEnabledProvider.builder()
                .profileFile(profileFile)
                .profileName(profile)
                .build();
        DualstackEnabledProvider dualstack = DualstackEnabledProvider.builder()
                .profileFile(profileFile)
                .profileName(profile)
                .build();
        boolean fipsEnabled = fips.isFipsEnabled().orElse(false);
        boolean dualstackEnabled = dualstack.isDualstackEnabled().orElse(false);
        return new ProfileEndpointSettings(fipsEnabled, dualstackEnabled);
    }
}
