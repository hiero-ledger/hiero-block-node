// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.suites.metrics;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for accessing and retrieving metrics from the Block-Node application.
 * This class provides methods to fetch metric values based on metric names and types,
 * handling both labeled and unlabeled metrics.
 */
public class MetricsAccessor {

    private final String metricsEndpoint;
    private final HttpClient client;

    public MetricsAccessor(String metricsEndpoint) {
        this.metricsEndpoint = metricsEndpoint;
        this.client = HttpClient.newBuilder().build();
    }

    /**
     * Retrieves the value of a specified metric from the Block-Node application.
     *
     * @param metricName The name of the metric to retrieve.
     * @param type       The type of the metric (COUNTER or GAUGE).
     * @return The value of the metric as a long.
     * @throws IOException          If an error occurs while fetching metrics.
     * @throws InterruptedException If the operation is interrupted.
     */
    public long getMetricValue(final String metricName, final MetricType type)
            throws IOException, InterruptedException {
        return getMetricValue(metricName, type, Map.of());
    }

    /**
     * Retrieves the value of a specified metric with labels from the Block-Node application.
     *
     * @param metricName The name of the metric to retrieve.
     * @param type       The type of the metric (COUNTER or GAUGE).
     * @param labels     A map of labels to filter the metric.
     * @return The value of the metric as a long.
     * @throws IOException          If an error occurs while fetching metrics.
     * @throws InterruptedException If the operation is interrupted.
     */
    public long getMetricValue(final String metricName, final MetricType type, final Map<String, String> labels)
            throws IOException, InterruptedException {
        final String metricPrefix = "hiero_block_node_";
        final String fullMetricName = metricPrefix + metricName + (type == MetricType.COUNTER ? "_total" : "");

        HttpRequest request =
                HttpRequest.newBuilder().uri(URI.create(metricsEndpoint)).GET().build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException("Failed to fetch metrics, status code: " + response.statusCode());
        }

        String responseBody = response.body();

        // Build pattern based on fullMetricName and labels
        StringBuilder labelPattern = new StringBuilder("\\{");
        labels.forEach((label, value) ->
                labelPattern.append(label).append("=\"").append(value).append("\","));
        labelPattern.deleteCharAt(labelPattern.length() - 1); // Remove trailing comma
        labelPattern.append("\\}");

        // Create pattern for metric with labels
        Pattern patternWithLabels = Pattern.compile(
                Pattern.quote(fullMetricName) + labelPattern.toString() + "\\s+([\\d\\.]+)", Pattern.MULTILINE);
        Matcher labelsMatcher = patternWithLabels.matcher(responseBody);

        if (labelsMatcher.find()) {
            return Math.round(Double.parseDouble(labelsMatcher.group(1)));
        }

        // Create pattern for metric without labels
        Pattern patternNoLabels =
                Pattern.compile("^" + Pattern.quote(fullMetricName) + "\\s+([\\d\\.]+)", Pattern.MULTILINE);
        Matcher noLabelsMatcher = patternNoLabels.matcher(responseBody);

        if (noLabelsMatcher.find()) {
            return Math.round(Double.parseDouble(noLabelsMatcher.group(1)));
        }

        throw new IOException("Metric not found: " + fullMetricName);
    }

    /**
     * Closes the HTTP client used for fetching metrics.
     */
    public void close() {
        client.close();
    }

    /**
     * Enum representing the type of metric.
     * It can be either COUNTER or GAUGE.
     */
    public enum MetricType {
        COUNTER,
        GAUGE,
    }
}
