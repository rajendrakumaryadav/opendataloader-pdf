/*
 * Copyright 2025 Hancom Inc.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.opendataloader.pdf.hybrid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opendataloader.pdf.containers.StaticLayoutContainers;
import org.opendataloader.pdf.entities.SemanticFormula;
import org.opendataloader.pdf.entities.SemanticPicture;
import org.opendataloader.pdf.hybrid.HybridClient.HybridResponse;
import org.verapdf.wcag.algorithms.entities.IObject;
import org.verapdf.wcag.algorithms.entities.SemanticHeading;
import org.verapdf.wcag.algorithms.entities.SemanticParagraph;
import org.verapdf.wcag.algorithms.entities.tables.tableBorders.TableBorder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for AzureSchemaTransformer.
 *
 * <p>Tests the transformation of Azure Document Intelligence analyzeResult
 * JSON format to OpenDataLoader IObject hierarchy.
 */
public class AzureSchemaTransformerTest {

    private AzureSchemaTransformer transformer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        transformer = new AzureSchemaTransformer();
        objectMapper = new ObjectMapper();
        StaticLayoutContainers.setCurrentContentId(1L);
    }

    @Test
    void testGetBackendType() {
        Assertions.assertEquals("azure", transformer.getBackendType());
    }

    @Test
    void testTransformNullJson() {
        HybridResponse response = new HybridResponse("", null, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    void testTransformEmptyJson() {
        ObjectNode json = createAnalyzeResult();

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0).isEmpty());
    }

    @Test
    void testTransformSimpleParagraph() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode paragraphs = json.putArray("paragraphs");

        addParagraph(paragraphs, "Hello World", null, 1, 1.0, 1.0, 3.0, 1.0, 3.0, 1.5, 1.0, 1.5);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertTrue(result.get(0).get(0) instanceof SemanticParagraph);

        SemanticParagraph paragraph = (SemanticParagraph) result.get(0).get(0);
        Assertions.assertEquals("Hello World", paragraph.getValue());
    }

    @Test
    void testTransformSectionHeading() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode paragraphs = json.putArray("paragraphs");

        addParagraph(paragraphs, "Introduction", "sectionHeading", 1, 1.0, 1.0, 4.0, 1.0, 4.0, 1.5, 1.0, 1.5);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertTrue(result.get(0).get(0) instanceof SemanticHeading);

        SemanticHeading heading = (SemanticHeading) result.get(0).get(0);
        Assertions.assertEquals("Introduction", heading.getValue());
    }

    @Test
    void testTransformTitle() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode paragraphs = json.putArray("paragraphs");

        addParagraph(paragraphs, "Document Title", "title", 1, 1.0, 0.5, 5.0, 0.5, 5.0, 1.0, 1.0, 1.0);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertTrue(result.get(0).get(0) instanceof SemanticHeading);
    }

    @Test
    void testFilterPageHeaderFooter() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode paragraphs = json.putArray("paragraphs");

        // Add page header - should be filtered
        addParagraph(paragraphs, "Chapter 1", "pageHeader", 1, 1.0, 0.3, 3.0, 0.3, 3.0, 0.5, 1.0, 0.5);

        // Add page footer - should be filtered
        addParagraph(paragraphs, "Page 1", "pageFooter", 1, 1.0, 10.5, 3.0, 10.5, 3.0, 10.8, 1.0, 10.8);

        // Add page number - should be filtered
        addParagraph(paragraphs, "1", "pageNumber", 1, 4.0, 10.5, 4.5, 10.5, 4.5, 10.8, 4.0, 10.8);

        // Add regular text - should be kept
        addParagraph(paragraphs, "Content", null, 1, 1.0, 2.0, 5.0, 2.0, 5.0, 2.5, 1.0, 2.5);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertTrue(result.get(0).get(0) instanceof SemanticParagraph);
    }

    @Test
    void testTransformSimpleTable() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode tables = json.putArray("tables");

        ObjectNode table = tables.addObject();
        table.put("rowCount", 2);
        table.put("columnCount", 2);

        // Add bounding region
        addBoundingRegion(table, 1, 1.0, 2.0, 5.0, 2.0, 5.0, 4.0, 1.0, 4.0);

        // Add cells
        ArrayNode cells = table.putArray("cells");
        addTableCell(cells, 0, 0, "A1", 1, 1);
        addTableCell(cells, 0, 1, "B1", 1, 1);
        addTableCell(cells, 1, 0, "A2", 1, 1);
        addTableCell(cells, 1, 1, "B2", 1, 1);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertTrue(result.get(0).get(0) instanceof TableBorder);

        TableBorder tableBorder = (TableBorder) result.get(0).get(0);
        Assertions.assertEquals(2, tableBorder.getNumberOfRows());
        Assertions.assertEquals(2, tableBorder.getNumberOfColumns());
    }

    @Test
    void testTransformFigure() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode figures = json.putArray("figures");

        ObjectNode figure = figures.addObject();
        addBoundingRegion(figure, 1, 1.0, 3.0, 5.0, 3.0, 5.0, 6.0, 1.0, 6.0);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertTrue(result.get(0).get(0) instanceof SemanticPicture);
    }

    @Test
    void testTransformFormula() {
        ObjectNode json = createAnalyzeResult();

        // Add formula to page
        ArrayNode pages = (ArrayNode) json.get("pages");
        ObjectNode page = (ObjectNode) pages.get(0);
        ArrayNode formulas = page.putArray("formulas");

        ObjectNode formula = formulas.addObject();
        formula.put("value", "E = mc^2");
        ArrayNode polygon = formula.putArray("polygon");
        polygon.add(2.0); polygon.add(3.0);
        polygon.add(4.0); polygon.add(3.0);
        polygon.add(4.0); polygon.add(3.5);
        polygon.add(2.0); polygon.add(3.5);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertTrue(result.get(0).get(0) instanceof SemanticFormula);

        SemanticFormula formulaObj = (SemanticFormula) result.get(0).get(0);
        Assertions.assertEquals("E = mc^2", formulaObj.getLatex());
    }

    @Test
    void testTransformMultiplePages() {
        ObjectNode json = createAnalyzeResult();

        // Add second page
        ArrayNode pages = (ArrayNode) json.get("pages");
        ObjectNode page2 = pages.addObject();
        page2.put("pageNumber", 2);
        page2.put("width", 8.5);
        page2.put("height", 11.0);
        page2.put("unit", "inch");

        ArrayNode paragraphs = json.putArray("paragraphs");

        // Text on page 1
        addParagraph(paragraphs, "Page 1 content", null, 1, 1.0, 1.0, 5.0, 1.0, 5.0, 1.5, 1.0, 1.5);

        // Text on page 2
        addParagraph(paragraphs, "Page 2 content", null, 2, 1.0, 1.0, 5.0, 1.0, 5.0, 1.5, 1.0, 1.5);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);
        pageHeights.put(2, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertEquals(1, result.get(1).size());

        SemanticParagraph p1 = (SemanticParagraph) result.get(0).get(0);
        SemanticParagraph p2 = (SemanticParagraph) result.get(1).get(0);

        Assertions.assertEquals("Page 1 content", p1.getValue());
        Assertions.assertEquals("Page 2 content", p2.getValue());
    }

    @Test
    void testBoundingBoxTransformation() {
        // Azure uses inches from top-left. For a US Letter page (8.5x11 inches = 612x792 points):
        // Polygon at (1.0, 1.0) to (3.0, 1.5) in inches
        // In points: x: 72..216, y_top_origin: 72..108
        // In BOTTOMLEFT: left=72, right=216, top=792-72=720, bottom=792-108=684
        ObjectNode json = createAnalyzeResult();
        ArrayNode paragraphs = json.putArray("paragraphs");

        addParagraph(paragraphs, "Test", null, 1, 1.0, 1.0, 3.0, 1.0, 3.0, 1.5, 1.0, 1.5);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());

        IObject obj = result.get(0).get(0);
        Assertions.assertEquals(72.0, obj.getLeftX(), 0.01);    // 1.0 * 72
        Assertions.assertEquals(216.0, obj.getRightX(), 0.01);   // 3.0 * 72
        Assertions.assertEquals(720.0, obj.getTopY(), 0.01);     // 792 - (1.0 * 72)
        Assertions.assertEquals(684.0, obj.getBottomY(), 0.01);  // 792 - (1.5 * 72)
    }

    @Test
    void testReadingOrderSort() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode paragraphs = json.putArray("paragraphs");

        // Add texts in reverse order (bottom to top in top-left coords)
        addParagraph(paragraphs, "Third", null, 1, 1.0, 8.0, 5.0, 8.0, 5.0, 8.5, 1.0, 8.5);   // bottom
        addParagraph(paragraphs, "First", null, 1, 1.0, 1.0, 5.0, 1.0, 5.0, 1.5, 1.0, 1.5);    // top
        addParagraph(paragraphs, "Second", null, 1, 1.0, 4.0, 5.0, 4.0, 5.0, 4.5, 1.0, 4.5);   // middle

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(3, result.get(0).size());

        // Should be sorted top to bottom (highest topY first)
        SemanticParagraph p1 = (SemanticParagraph) result.get(0).get(0);
        SemanticParagraph p2 = (SemanticParagraph) result.get(0).get(1);
        SemanticParagraph p3 = (SemanticParagraph) result.get(0).get(2);

        Assertions.assertEquals("First", p1.getValue());
        Assertions.assertEquals("Second", p2.getValue());
        Assertions.assertEquals("Third", p3.getValue());
    }

    @Test
    void testMixedContent() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode paragraphs = json.putArray("paragraphs");
        ArrayNode tables = json.putArray("tables");
        ArrayNode figures = json.putArray("figures");

        // Add heading at top
        addParagraph(paragraphs, "Title", "title", 1, 1.0, 0.5, 5.0, 0.5, 5.0, 1.0, 1.0, 1.0);

        // Add paragraph in middle
        addParagraph(paragraphs, "Body text", null, 1, 1.0, 2.0, 5.0, 2.0, 5.0, 2.5, 1.0, 2.5);

        // Add table at bottom
        ObjectNode table = tables.addObject();
        table.put("rowCount", 1);
        table.put("columnCount", 1);
        addBoundingRegion(table, 1, 1.0, 5.0, 5.0, 5.0, 5.0, 7.0, 1.0, 7.0);
        ArrayNode cells = table.putArray("cells");
        addTableCell(cells, 0, 0, "Cell", 1, 1);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(3, result.get(0).size());

        // Sorted by reading order: heading (top), paragraph, table (bottom)
        Assertions.assertTrue(result.get(0).get(0) instanceof SemanticHeading);
        Assertions.assertTrue(result.get(0).get(1) instanceof SemanticParagraph);
        Assertions.assertTrue(result.get(0).get(2) instanceof TableBorder);
    }

    @Test
    void testTransformPage() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode paragraphs = json.putArray("paragraphs");

        addParagraph(paragraphs, "Single page content", null, 1, 1.0, 1.0, 5.0, 1.0, 5.0, 1.5, 1.0, 1.5);

        List<IObject> result = transformer.transformPage(1, json, 792.0);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.get(0) instanceof SemanticParagraph);
    }

    @Test
    void testTableWithSpans() {
        ObjectNode json = createAnalyzeResult();
        ArrayNode tables = json.putArray("tables");

        ObjectNode table = tables.addObject();
        table.put("rowCount", 2);
        table.put("columnCount", 2);

        addBoundingRegion(table, 1, 1.0, 2.0, 5.0, 2.0, 5.0, 4.0, 1.0, 4.0);

        ArrayNode cells = table.putArray("cells");
        // First cell spans 2 columns
        addTableCellWithSpan(cells, 0, 0, "Header", 1, 2);
        addTableCell(cells, 1, 0, "A2", 1, 1);
        addTableCell(cells, 1, 1, "B2", 1, 1);

        HybridResponse response = new HybridResponse("", json, null);
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(1, 792.0);

        List<List<IObject>> result = transformer.transform(response, pageHeights);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).size());
        Assertions.assertTrue(result.get(0).get(0) instanceof TableBorder);

        TableBorder tableBorder = (TableBorder) result.get(0).get(0);
        Assertions.assertEquals(2, tableBorder.getNumberOfRows());
        Assertions.assertEquals(2, tableBorder.getNumberOfColumns());
        Assertions.assertEquals(2, tableBorder.getRow(0).getCell(0).getColSpan());
    }

    // Helper methods

    private ObjectNode createAnalyzeResult() {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("apiVersion", "2024-11-30");
        json.put("modelId", "prebuilt-layout");

        ArrayNode pages = json.putArray("pages");
        ObjectNode page1 = pages.addObject();
        page1.put("pageNumber", 1);
        page1.put("width", 8.5);
        page1.put("height", 11.0);
        page1.put("unit", "inch");

        return json;
    }

    private void addParagraph(ArrayNode paragraphs, String content, String role, int pageNumber,
                               double x1, double y1, double x2, double y2,
                               double x3, double y3, double x4, double y4) {
        ObjectNode para = paragraphs.addObject();
        para.put("content", content);
        if (role != null) {
            para.put("role", role);
        }

        addBoundingRegion(para, pageNumber, x1, y1, x2, y2, x3, y3, x4, y4);
    }

    private void addBoundingRegion(ObjectNode node, int pageNumber,
                                    double x1, double y1, double x2, double y2,
                                    double x3, double y3, double x4, double y4) {
        ArrayNode regions = node.putArray("boundingRegions");
        ObjectNode region = regions.addObject();
        region.put("pageNumber", pageNumber);

        ArrayNode polygon = region.putArray("polygon");
        polygon.add(x1); polygon.add(y1);
        polygon.add(x2); polygon.add(y2);
        polygon.add(x3); polygon.add(y3);
        polygon.add(x4); polygon.add(y4);
    }

    private void addTableCell(ArrayNode cells, int row, int col, String content,
                               int rowSpan, int colSpan) {
        ObjectNode cell = cells.addObject();
        cell.put("rowIndex", row);
        cell.put("columnIndex", col);
        cell.put("content", content);
        if (rowSpan > 1) {
            cell.put("rowSpan", rowSpan);
        }
        if (colSpan > 1) {
            cell.put("columnSpan", colSpan);
        }
    }

    private void addTableCellWithSpan(ArrayNode cells, int row, int col, String content,
                                       int rowSpan, int colSpan) {
        ObjectNode cell = cells.addObject();
        cell.put("rowIndex", row);
        cell.put("columnIndex", col);
        cell.put("content", content);
        cell.put("rowSpan", rowSpan);
        cell.put("columnSpan", colSpan);
    }
}
