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
package io.tileverse.rangereader.benchmarks;

import io.tileverse.rangereader.RangeReader;
import io.tileverse.rangereader.http.HttpRangeReader;
import java.io.IOException;
import java.net.URI;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.RunnerException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * JMH benchmark for HttpRangeReader with various configurations.
 * <p>
 * This benchmark measures the performance of HttpRangeReader with different
 * combinations of caching and block alignment. It uses an Nginx container to
 * serve the test file over HTTP.
 */
@State(Scope.Benchmark)
public class HttpRangeReaderBenchmark extends AbstractRangeReaderBenchmark {

    /**
     * Nginx container for HTTP requests.
     */
    private GenericContainer<?> nginx;

    /**
     * URI for the test file on the Nginx server.
     */
    private URI testFileUri; // =
    // URI.create("https://overturemaps-tiles-us-west-2-beta.s3.amazonaws.com/2024-08-20/places.pmtiles");

    /**
     * Setup method that creates an Nginx container and configures it with our test
     * file. This runs once per entire benchmark.
     */
    @Setup(Level.Trial)
    @Override
    public void setupTrial() throws IOException {
        // Call the parent setup to create the test file
        super.setupTrial();

        // Start Nginx container
        nginx = new GenericContainer<>(DockerImageName.parse("nginx:alpine"))
                .withCommand(
                        "sh",
                        "-c",
                        """
				# Create the HTML directory
				mkdir -p /usr/share/nginx/html

				# Set up Nginx config for range requests
				cat > /etc/nginx/conf.d/default.conf << 'EOF'
				server {
				    listen 80;
				    server_name localhost;
				    location / {
				        root /usr/share/nginx/html;
				        autoindex on;
				        add_header Accept-Ranges bytes;
				    }
				}
				EOF

				# Start Nginx
				nginx -g 'daemon off;'
				""")
                .withExposedPorts(80)
                .waitingFor(Wait.forHttp("/"));

        nginx.start();

        // Copy the test file to Nginx container
        nginx.copyFileToContainer(
                org.testcontainers.utility.MountableFile.forHostPath(testFilePath),
                "/usr/share/nginx/html/" + testFilePath.getFileName());

        // Set the test file URI
        testFileUri = URI.create(String.format(
                "http://%s:%d/%s",
                nginx.getHost(),
                nginx.getMappedPort(80),
                testFilePath.getFileName().toString()));
    }

    /**
     * Teardown method that stops the Nginx container.
     */
    @TearDown(Level.Trial)
    @Override
    public void teardownTrial() throws IOException {
        // Call the parent teardown to clean up the test file
        super.teardownTrial();

        // Stop Nginx container
        if (nginx != null) {
            nginx.stop();
        }
    }

    @Override
    protected RangeReader createBaseReader() throws IOException {
        return HttpRangeReader.builder().uri(testFileUri).build();
    }

    @Override
    protected String getSourceIndentifier() {
        return testFileUri.toString();
    }

    /**
     * Main method to run this benchmark.
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(HttpRangeReaderBenchmark.class);
    }
}
