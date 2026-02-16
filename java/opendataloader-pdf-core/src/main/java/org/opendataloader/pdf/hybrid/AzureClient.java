/*
 * Copyright 2025 Hancom Inc.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.opendataloader.pdf.hybrid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP client for Azure Document Intelligence (formerly Form Recognizer) API.
 *
 * <p>This client communicates with the Azure Document Intelligence REST API
 * using the prebuilt-layout model for PDF analysis. The workflow is:
 * <ol>
 *   <li>POST PDF to analyze endpoint (returns operation-location header)</li>
 *   <li>Poll the operation URL until analysis completes</li>
 *   <li>Parse the analysis result into HybridResponse</li>
 * </ol>
 *
 * <p>Authentication is via {@code Ocp-Apim-Subscription-Key} header using
 * the API key from the hybrid configuration.
 *
 * <h2>API Version</h2>
 * <p>This client targets the Azure Document Intelligence REST API version
 * {@code 2024-11-30} (GA).
 *
 * <h2>Usage</h2>
 * <pre>
 * # Set environment variable or use --hybrid-api-key CLI option
 * export AZURE_API_KEY=your-api-key
 * opendataloader-pdf --hybrid azure --hybrid-url https://your-resource.cognitiveservices.azure.com input.pdf
 * </pre>
 *
 * @see HybridClient
 * @see HybridConfig
 */
public class AzureClient implements HybridClient {

    private static final Logger LOGGER = Logger.getLogger(AzureClient.class.getCanonicalName());

    /** Default API version for Azure Document Intelligence. */
    static final String API_VERSION = "2024-11-30";

    /** Analyze endpoint path for prebuilt-layout model. */
    static final String ANALYZE_ENDPOINT =
        "/documentintelligence/documentModels/prebuilt-layout:analyze?api-version=" + API_VERSION;

    /** Header name for Azure API key authentication. */
    static final String API_KEY_HEADER = "Ocp-Apim-Subscription-Key";

    /** Header name for the operation location returned by the analyze call. */
    static final String OPERATION_LOCATION_HEADER = "Operation-Location";

    private static final String DEFAULT_FILENAME = "document.pdf";
    private static final MediaType MEDIA_TYPE_PDF = MediaType.parse("application/pdf");

    /** Poll interval in milliseconds when waiting for analysis to complete. */
    private static final long POLL_INTERVAL_MS = 1000;

    /** Maximum number of poll attempts before timing out. */
    private static final int MAX_POLL_ATTEMPTS = 120;

    private final String baseUrl;
    private final String apiKey;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new AzureClient with the specified configuration.
     *
     * @param config The hybrid configuration containing URL, timeout, and API key settings.
     * @throws IllegalArgumentException if the API key is not configured.
     */
    public AzureClient(HybridConfig config) {
        this.baseUrl = normalizeUrl(config.getEffectiveUrl("azure"));
        this.apiKey = config.getApiKey();
        this.objectMapper = new ObjectMapper();
        this.httpClient = new OkHttpClient.Builder()
            .connectTimeout(config.getTimeoutMs(), TimeUnit.MILLISECONDS)
            .readTimeout(config.getTimeoutMs(), TimeUnit.MILLISECONDS)
            .writeTimeout(config.getTimeoutMs(), TimeUnit.MILLISECONDS)
            .build();

        if (this.baseUrl == null || this.baseUrl.isEmpty()) {
            throw new IllegalArgumentException(
                "Azure Document Intelligence requires a URL. " +
                "Use --hybrid-url to specify your Azure endpoint " +
                "(e.g., https://your-resource.cognitiveservices.azure.com)");
        }

        if (this.apiKey == null || this.apiKey.isEmpty()) {
            throw new IllegalArgumentException(
                "Azure Document Intelligence requires an API key. " +
                "Use --hybrid-api-key or set the AZURE_API_KEY environment variable.");
        }
    }

    /**
     * Creates a new AzureClient with a custom OkHttpClient (for testing).
     *
     * @param baseUrl      The base URL of the Azure endpoint.
     * @param apiKey       The API key for authentication.
     * @param httpClient   The OkHttp client to use for requests.
     * @param objectMapper The Jackson ObjectMapper for JSON parsing.
     */
    AzureClient(String baseUrl, String apiKey, OkHttpClient httpClient, ObjectMapper objectMapper) {
        this.baseUrl = normalizeUrl(baseUrl);
        this.apiKey = apiKey;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    @Override
    public HybridResponse convert(HybridRequest request) throws IOException {
        // Step 1: Submit PDF for analysis
        String operationUrl = submitAnalysis(request.getPdfBytes());
        LOGGER.log(Level.FINE, "Submitted analysis, operation URL: {0}", operationUrl);

        // Step 2: Poll for result
        JsonNode result = pollForResult(operationUrl);
        LOGGER.log(Level.FINE, "Analysis completed successfully");

        // Step 3: Build response
        return buildResponse(result);
    }

    @Override
    public CompletableFuture<HybridResponse> convertAsync(HybridRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return convert(request);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to convert", e);
            }
        });
    }

    /**
     * Gets the base URL of this client.
     *
     * @return The base URL.
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     * Submits a PDF document for analysis.
     *
     * @param pdfBytes The PDF file bytes.
     * @return The operation URL to poll for results.
     * @throws IOException If the submission fails.
     */
    private String submitAnalysis(byte[] pdfBytes) throws IOException {
        Request request = new Request.Builder()
            .url(baseUrl + ANALYZE_ENDPOINT)
            .header(API_KEY_HEADER, apiKey)
            .header("Content-Type", "application/pdf")
            .post(RequestBody.create(pdfBytes, MEDIA_TYPE_PDF))
            .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                ResponseBody body = response.body();
                String bodyStr = body != null ? body.string() : "";
                throw new IOException(
                    "Azure Document Intelligence analyze request failed with status " +
                    response.code() + ": " + bodyStr);
            }

            String operationLocation = response.header(OPERATION_LOCATION_HEADER);
            if (operationLocation == null || operationLocation.isEmpty()) {
                throw new IOException(
                    "Azure Document Intelligence response missing " +
                    OPERATION_LOCATION_HEADER + " header");
            }

            return operationLocation;
        }
    }

    /**
     * Polls the operation URL until the analysis completes or fails.
     *
     * @param operationUrl The URL to poll for results.
     * @return The analysis result JSON.
     * @throws IOException If polling fails or analysis does not complete.
     */
    private JsonNode pollForResult(String operationUrl) throws IOException {
        for (int attempt = 0; attempt < MAX_POLL_ATTEMPTS; attempt++) {
            Request request = new Request.Builder()
                .url(operationUrl)
                .header(API_KEY_HEADER, apiKey)
                .get()
                .build();

            try (Response response = httpClient.newCall(request).execute()) {
                if (!response.isSuccessful()) {
                    ResponseBody body = response.body();
                    String bodyStr = body != null ? body.string() : "";
                    throw new IOException(
                        "Azure Document Intelligence poll failed with status " +
                        response.code() + ": " + bodyStr);
                }

                ResponseBody body = response.body();
                if (body == null) {
                    throw new IOException("Empty response body from poll");
                }

                JsonNode root = objectMapper.readTree(body.string());
                String status = root.has("status") ? root.get("status").asText() : "";

                if ("succeeded".equals(status)) {
                    JsonNode analyzeResult = root.get("analyzeResult");
                    if (analyzeResult == null) {
                        throw new IOException(
                            "Azure Document Intelligence response missing analyzeResult");
                    }
                    return analyzeResult;
                } else if ("failed".equals(status)) {
                    JsonNode error = root.get("error");
                    String errorMessage = error != null ? error.toString() : "Unknown error";
                    throw new IOException(
                        "Azure Document Intelligence analysis failed: " + errorMessage);
                }

                // Status is "running" or "notStarted" — wait and retry
                try {
                    Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Polling interrupted", e);
                }
            }
        }

        throw new IOException(
            "Azure Document Intelligence analysis timed out after " +
            MAX_POLL_ATTEMPTS + " poll attempts");
    }

    /**
     * Builds a HybridResponse from the Azure analyzeResult JSON.
     *
     * @param analyzeResult The analyzeResult node from the Azure response.
     * @return A HybridResponse with the JSON content and per-page data.
     */
    private HybridResponse buildResponse(JsonNode analyzeResult) {
        Map<Integer, JsonNode> pageContents = new HashMap<>();

        JsonNode pages = analyzeResult.get("pages");
        if (pages != null && pages.isArray()) {
            for (JsonNode page : pages) {
                int pageNumber = page.has("pageNumber") ? page.get("pageNumber").asInt() : 0;
                if (pageNumber > 0) {
                    pageContents.put(pageNumber, page);
                }
            }
        }

        return new HybridResponse(null, null, analyzeResult, pageContents);
    }

    /**
     * Normalizes a URL by removing trailing slashes.
     */
    private static String normalizeUrl(String url) {
        if (url != null && url.endsWith("/")) {
            return url.substring(0, url.length() - 1);
        }
        return url;
    }

    /**
     * Shuts down the HTTP client and releases all resources.
     */
    public void shutdown() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
        if (httpClient.cache() != null) {
            try {
                httpClient.cache().close();
            } catch (Exception ignored) {
                // Ignore cache close errors
            }
        }
    }
}
