/*
 * Copyright 2025 Hancom Inc.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.opendataloader.pdf.hybrid;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opendataloader.pdf.hybrid.HybridClient.HybridRequest;
import org.opendataloader.pdf.hybrid.HybridClient.HybridResponse;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for AzureClient.
 *
 * <p>Uses MockWebServer to simulate Azure Document Intelligence API responses.
 */
public class AzureClientTest {

    private MockWebServer mockServer;
    private AzureClient client;
    private ObjectMapper objectMapper;

    private static final byte[] SAMPLE_PDF_BYTES = "%PDF-1.4 sample".getBytes();
    private static final String TEST_API_KEY = "test-api-key-12345";

    @BeforeEach
    void setUp() throws IOException {
        mockServer = new MockWebServer();
        mockServer.start();

        HybridConfig config = new HybridConfig();
        config.setUrl(mockServer.url("/").toString());
        config.setApiKey(TEST_API_KEY);
        config.setTimeoutMs(5000);

        client = new AzureClient(config);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void tearDown() throws IOException {
        client.shutdown();
        mockServer.shutdown();
    }

    @Test
    void testMissingUrlThrowsIllegalArgument() {
        HybridConfig config = new HybridConfig();
        config.setApiKey(TEST_API_KEY);
        // No URL set

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new AzureClient(config);
        });
    }

    @Test
    void testMissingApiKeyThrowsIllegalArgument() {
        HybridConfig config = new HybridConfig();
        config.setUrl("https://example.cognitiveservices.azure.com");
        // No API key set

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            new AzureClient(config);
        });
    }

    @Test
    void testConvertFullWorkflow() throws Exception {
        // Mock analyze response (202 with Operation-Location header)
        String operationUrl = mockServer.url("/results/operation-123").toString();
        mockServer.enqueue(new MockResponse()
            .setResponseCode(202)
            .setHeader("Operation-Location", operationUrl));

        // Mock poll response - succeeded
        String analyzeResult = createSuccessfulAnalyzeResult();
        mockServer.enqueue(new MockResponse()
            .setBody(analyzeResult)
            .setHeader("Content-Type", "application/json"));

        HybridRequest request = HybridRequest.allPages(SAMPLE_PDF_BYTES);
        HybridResponse response = client.convert(request);

        Assertions.assertNotNull(response);
        Assertions.assertNotNull(response.getJson());
        Assertions.assertFalse(response.getPageContents().isEmpty());
        Assertions.assertTrue(response.getPageContents().containsKey(1));

        // Verify 2 requests: analyze + poll
        Assertions.assertEquals(2, mockServer.getRequestCount());

        // Verify analyze request
        RecordedRequest analyzeReq = mockServer.takeRequest(1, TimeUnit.SECONDS);
        Assertions.assertTrue(analyzeReq.getPath().contains("documentModels/prebuilt-layout:analyze"));
        Assertions.assertEquals(TEST_API_KEY, analyzeReq.getHeader("Ocp-Apim-Subscription-Key"));
        Assertions.assertEquals("application/pdf", analyzeReq.getHeader("Content-Type"));

        // Verify poll request
        RecordedRequest pollReq = mockServer.takeRequest(1, TimeUnit.SECONDS);
        Assertions.assertEquals(TEST_API_KEY, pollReq.getHeader("Ocp-Apim-Subscription-Key"));
    }

    @Test
    void testConvertWithPolling() throws Exception {
        // Mock analyze response
        String operationUrl = mockServer.url("/results/operation-456").toString();
        mockServer.enqueue(new MockResponse()
            .setResponseCode(202)
            .setHeader("Operation-Location", operationUrl));

        // Mock poll - running
        mockServer.enqueue(new MockResponse()
            .setBody("{\"status\":\"running\"}")
            .setHeader("Content-Type", "application/json"));

        // Mock poll - succeeded
        String analyzeResult = createSuccessfulAnalyzeResult();
        mockServer.enqueue(new MockResponse()
            .setBody(analyzeResult)
            .setHeader("Content-Type", "application/json"));

        HybridRequest request = HybridRequest.allPages(SAMPLE_PDF_BYTES);
        HybridResponse response = client.convert(request);

        Assertions.assertNotNull(response);
        // 3 requests: analyze + running + succeeded
        Assertions.assertEquals(3, mockServer.getRequestCount());
    }

    @Test
    void testConvertAnalyzeFailure() throws Exception {
        // Mock analyze error
        mockServer.enqueue(new MockResponse()
            .setResponseCode(401)
            .setBody("{\"error\":{\"code\":\"Unauthorized\",\"message\":\"Invalid API key\"}}"));

        HybridRequest request = HybridRequest.allPages(SAMPLE_PDF_BYTES);

        Assertions.assertThrows(IOException.class, () -> {
            client.convert(request);
        });

        Assertions.assertEquals(1, mockServer.getRequestCount());
    }

    @Test
    void testConvertAnalysisFailedStatus() throws Exception {
        // Mock analyze response
        String operationUrl = mockServer.url("/results/operation-fail").toString();
        mockServer.enqueue(new MockResponse()
            .setResponseCode(202)
            .setHeader("Operation-Location", operationUrl));

        // Mock poll - failed
        mockServer.enqueue(new MockResponse()
            .setBody("{\"status\":\"failed\",\"error\":{\"code\":\"InternalError\",\"message\":\"Processing failed\"}}")
            .setHeader("Content-Type", "application/json"));

        HybridRequest request = HybridRequest.allPages(SAMPLE_PDF_BYTES);

        IOException exception = Assertions.assertThrows(IOException.class, () -> {
            client.convert(request);
        });
        Assertions.assertTrue(exception.getMessage().contains("analysis failed"));
    }

    @Test
    void testConvertMissingOperationLocation() throws Exception {
        // Mock analyze response without Operation-Location header
        mockServer.enqueue(new MockResponse()
            .setResponseCode(202));

        HybridRequest request = HybridRequest.allPages(SAMPLE_PDF_BYTES);

        IOException exception = Assertions.assertThrows(IOException.class, () -> {
            client.convert(request);
        });
        Assertions.assertTrue(exception.getMessage().contains("Operation-Location"));
    }

    @Test
    void testConvertAsync() throws Exception {
        // Mock analyze response
        String operationUrl = mockServer.url("/results/operation-async").toString();
        mockServer.enqueue(new MockResponse()
            .setResponseCode(202)
            .setHeader("Operation-Location", operationUrl));

        // Mock poll - succeeded
        String analyzeResult = createSuccessfulAnalyzeResult();
        mockServer.enqueue(new MockResponse()
            .setBody(analyzeResult)
            .setHeader("Content-Type", "application/json"));

        HybridRequest request = HybridRequest.allPages(SAMPLE_PDF_BYTES);
        HybridResponse response = client.convertAsync(request).get(10, TimeUnit.SECONDS);

        Assertions.assertNotNull(response);
    }

    @Test
    void testGetBaseUrl() {
        Assertions.assertNotNull(client.getBaseUrl());
        Assertions.assertFalse(client.getBaseUrl().isEmpty());
    }

    private String createSuccessfulAnalyzeResult() {
        return "{\n" +
            "  \"status\": \"succeeded\",\n" +
            "  \"analyzeResult\": {\n" +
            "    \"apiVersion\": \"2024-11-30\",\n" +
            "    \"modelId\": \"prebuilt-layout\",\n" +
            "    \"pages\": [\n" +
            "      {\n" +
            "        \"pageNumber\": 1,\n" +
            "        \"width\": 8.5,\n" +
            "        \"height\": 11,\n" +
            "        \"unit\": \"inch\",\n" +
            "        \"words\": [],\n" +
            "        \"lines\": []\n" +
            "      }\n" +
            "    ],\n" +
            "    \"paragraphs\": [\n" +
            "      {\n" +
            "        \"content\": \"Hello World\",\n" +
            "        \"boundingRegions\": [\n" +
            "          {\n" +
            "            \"pageNumber\": 1,\n" +
            "            \"polygon\": [1.0, 1.0, 3.0, 1.0, 3.0, 1.5, 1.0, 1.5]\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    ],\n" +
            "    \"tables\": [],\n" +
            "    \"figures\": []\n" +
            "  }\n" +
            "}";
    }
}
