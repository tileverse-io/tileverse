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
package io.tileverse.storage.it;

import com.github.dockerjava.api.command.InspectContainerResponse;
import java.io.IOException;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * A single-node <a href="https://garagehq.deuxfleurs.fr/">Garage</a> S3-compatible server, ready to accept requests
 * signed with {@link #getAccessKeyId()} and {@link #getSecretAccessKey()} for region {@link #REGION}. The key may
 * create buckets and owns the buckets it creates.
 *
 * <p>Garage replaces MinIO as the self-hosted S3 test server because MinIO no longer publishes free container images.
 */
public class GarageContainer extends GenericContainer<GarageContainer> {

    public static final DockerImageName DEFAULT_IMAGE = DockerImageName.parse("dxflrs/garage:v2.4.1");

    public static final String REGION = "us-east-1";

    private static final int S3_PORT = 3900;

    // Garage accepts imported keys only in its own format: "GK" plus 24 hex digits, and a 64 hex digit secret.
    private static final String ACCESS_KEY_ID = "GK0123456789abcdef01234567";
    private static final String SECRET_ACCESS_KEY = "0123456789abcdef".repeat(4);

    private static final String CONFIG_PATH = "/etc/garage.toml";
    private static final String CONFIG = """
            metadata_dir = "/var/lib/garage/meta"
            data_dir = "/var/lib/garage/data"
            db_engine = "sqlite"
            replication_factor = 1
            rpc_bind_addr = "[::]:3901"
            rpc_secret = "%s"

            [s3_api]
            s3_region = "%s"
            api_bind_addr = "[::]:%d"
            """.formatted("0".repeat(64), REGION, S3_PORT);

    public GarageContainer() {
        this(DEFAULT_IMAGE);
    }

    public GarageContainer(DockerImageName image) {
        super(image);
        withExposedPorts(S3_PORT);
        withCopyToContainer(Transferable.of(CONFIG), CONFIG_PATH);
        waitingFor(Wait.forLogMessage(".*S3 API server listening.*", 1));
    }

    /** The S3 endpoint as seen from the host, e.g. {@code http://localhost:32768}. */
    public String getS3URL() {
        return "http://%s:%d".formatted(getHost(), getMappedPort(S3_PORT));
    }

    public String getAccessKeyId() {
        return ACCESS_KEY_ID;
    }

    public String getSecretAccessKey() {
        return SECRET_ACCESS_KEY;
    }

    /**
     * Garage serves no S3 requests until the node has a storage role in the cluster layout, and it has no built-in
     * credentials.
     */
    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        String nodeId = garage("node", "id", "--quiet").trim();
        garage("layout", "assign", "--zone", "dc1", "--capacity", "1G", nodeId);
        garage("layout", "apply", "--version", "1");
        garage("key", "import", "--yes", "-n", "test", ACCESS_KEY_ID, SECRET_ACCESS_KEY);
        garage("key", "allow", "--create-bucket", ACCESS_KEY_ID);
    }

    private String garage(String... args) {
        String[] command = new String[args.length + 1];
        command[0] = "/garage";
        System.arraycopy(args, 0, command, 1, args.length);
        ExecResult result = exec(command);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(
                    "garage %s failed: %s".formatted(String.join(" ", args), result.getStderr()));
        }
        return result.getStdout();
    }

    private ExecResult exec(String[] command) {
        try {
            return execInContainer(command);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
