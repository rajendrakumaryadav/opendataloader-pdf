/*
 * Copyright 2025 Hancom Inc.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.opendataloader.pdf.hybrid;

import com.fasterxml.jackson.databind.JsonNode;
import org.opendataloader.pdf.containers.StaticLayoutContainers;
import org.opendataloader.pdf.entities.SemanticFormula;
import org.opendataloader.pdf.entities.SemanticPicture;
import org.opendataloader.pdf.hybrid.HybridClient.HybridResponse;
import org.verapdf.wcag.algorithms.entities.IObject;
import org.verapdf.wcag.algorithms.entities.SemanticHeading;
import org.verapdf.wcag.algorithms.entities.SemanticParagraph;
import org.verapdf.wcag.algorithms.entities.content.TextChunk;
import org.verapdf.wcag.algorithms.entities.content.TextLine;
import org.verapdf.wcag.algorithms.entities.geometry.BoundingBox;
import org.verapdf.wcag.algorithms.entities.tables.tableBorders.TableBorder;
import org.verapdf.wcag.algorithms.entities.tables.tableBorders.TableBorderCell;
import org.verapdf.wcag.algorithms.entities.tables.tableBorders.TableBorderRow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Transforms Azure Document Intelligence analyzeResult JSON to OpenDataLoader IObject hierarchy.
 *
 * <p>This transformer handles the Azure Document Intelligence response format
 * (prebuilt-layout model) and converts its elements to the equivalent IObject
 * types used by OpenDataLoader's downstream processors and generators.
 *
 * <h2>Schema Mapping</h2>
 * <ul>
 *   <li>paragraphs (role: null/default) → SemanticParagraph</li>
 *   <li>paragraphs (role: sectionHeading) → SemanticHeading</li>
 *   <li>paragraphs (role: title) → SemanticHeading (level 1)</li>
 *   <li>paragraphs (role: pageHeader, pageFooter, pageNumber) → Filtered out (furniture)</li>
 *   <li>paragraphs (role: footnote) → SemanticParagraph</li>
 *   <li>tables → TableBorder with rows and cells</li>
 *   <li>figures → SemanticPicture</li>
 *   <li>formulas → SemanticFormula</li>
 * </ul>
 *
 * <h2>Coordinate System</h2>
 * <p>Azure Document Intelligence uses a coordinate system where (0,0) is at the
 * top-left of the page, with units in inches. Bounding boxes are specified as
 * polygon arrays [x1,y1,x2,y2,...,x4,y4] (4 points clockwise from top-left).
 * This transformer converts these to PDF points (1 inch = 72 points) and flips
 * the Y-axis to match OpenDataLoader's BOTTOMLEFT origin.
 *
 * <h2>Thread Safety</h2>
 * <p>This class is NOT thread-safe. The {@code transform()} method resets
 * internal state (pictureIndex) at the start of each call.
 * Use separate instances for concurrent transformations.
 */
public class AzureSchemaTransformer implements HybridSchemaTransformer {

    private static final Logger LOGGER = Logger.getLogger(AzureSchemaTransformer.class.getCanonicalName());

    private static final String BACKEND_TYPE = "azure";

    /** Conversion factor from inches to PDF points. */
    static final double INCHES_TO_POINTS = 72.0;

    // Picture index counter (reset per transform call)
    private int pictureIndex;

    // Azure paragraph roles
    private static final String ROLE_SECTION_HEADING = "sectionHeading";
    private static final String ROLE_TITLE = "title";
    private static final String ROLE_PAGE_HEADER = "pageHeader";
    private static final String ROLE_PAGE_FOOTER = "pageFooter";
    private static final String ROLE_PAGE_NUMBER = "pageNumber";
    private static final String ROLE_FOOTNOTE = "footnote";

    @Override
    public String getBackendType() {
        return BACKEND_TYPE;
    }

    @Override
    public List<List<IObject>> transform(HybridResponse response, Map<Integer, Double> pageHeights) {
        JsonNode json = response.getJson();
        if (json == null) {
            LOGGER.log(Level.WARNING, "HybridResponse JSON is null, returning empty result");
            return Collections.emptyList();
        }

        // Reset picture index for each transform call
        pictureIndex = 0;

        // Determine number of pages and collect page dimensions
        Map<Integer, double[]> pageDimensions = extractPageDimensions(json);
        int numPages = determinePageCount(json, pageHeights, pageDimensions);

        // Initialize result list
        List<List<IObject>> result = new ArrayList<>(numPages);
        for (int i = 0; i < numPages; i++) {
            result.add(new ArrayList<>());
        }

        // Transform paragraphs
        JsonNode paragraphs = json.get("paragraphs");
        if (paragraphs != null && paragraphs.isArray()) {
            for (JsonNode paragraph : paragraphs) {
                transformParagraph(paragraph, result, pageHeights, pageDimensions);
            }
        }

        // Transform tables
        JsonNode tables = json.get("tables");
        if (tables != null && tables.isArray()) {
            for (JsonNode table : tables) {
                transformTable(table, result, pageHeights, pageDimensions);
            }
        }

        // Transform figures
        JsonNode figures = json.get("figures");
        if (figures != null && figures.isArray()) {
            for (JsonNode figure : figures) {
                transformFigure(figure, result, pageHeights, pageDimensions);
            }
        }

        // Transform formulas (from pages[].formulas[])
        JsonNode pages = json.get("pages");
        if (pages != null && pages.isArray()) {
            for (JsonNode page : pages) {
                int pageNumber = page.has("pageNumber") ? page.get("pageNumber").asInt() : 1;
                JsonNode formulas = page.get("formulas");
                if (formulas != null && formulas.isArray()) {
                    for (JsonNode formula : formulas) {
                        transformFormula(formula, pageNumber, result, pageHeights, pageDimensions);
                    }
                }
            }
        }

        // Sort each page's contents by reading order
        for (List<IObject> pageContents : result) {
            sortByReadingOrder(pageContents);
        }

        return result;
    }

    @Override
    public List<IObject> transformPage(int pageNumber, JsonNode pageContent, double pageHeight) {
        Map<Integer, Double> pageHeights = new HashMap<>();
        pageHeights.put(pageNumber, pageHeight);

        HybridResponse singlePageResponse = new HybridResponse("", pageContent, Collections.emptyMap());
        List<List<IObject>> result = transform(singlePageResponse, pageHeights);

        if (result.isEmpty()) {
            return Collections.emptyList();
        }

        int pageIndex = pageNumber - 1;
        if (pageIndex >= 0 && pageIndex < result.size()) {
            return result.get(pageIndex);
        }

        return Collections.emptyList();
    }

    /**
     * Extracts page dimensions (width, height) from the Azure response.
     *
     * @param json The analyzeResult JSON.
     * @return Map of page number (1-indexed) to [width, height] in inches.
     */
    private Map<Integer, double[]> extractPageDimensions(JsonNode json) {
        Map<Integer, double[]> dimensions = new HashMap<>();

        JsonNode pages = json.get("pages");
        if (pages != null && pages.isArray()) {
            for (JsonNode page : pages) {
                int pageNumber = page.has("pageNumber") ? page.get("pageNumber").asInt() : 0;
                double width = page.has("width") ? page.get("width").asDouble() : 8.5;
                double height = page.has("height") ? page.get("height").asDouble() : 11.0;
                if (pageNumber > 0) {
                    dimensions.put(pageNumber, new double[]{width, height});
                }
            }
        }

        return dimensions;
    }

    /**
     * Determines the number of pages.
     */
    private int determinePageCount(JsonNode json, Map<Integer, Double> pageHeights,
                                    Map<Integer, double[]> pageDimensions) {
        if (pageHeights != null && !pageHeights.isEmpty()) {
            return pageHeights.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
        }

        if (!pageDimensions.isEmpty()) {
            return pageDimensions.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
        }

        JsonNode pages = json.get("pages");
        if (pages != null && pages.isArray()) {
            return pages.size();
        }

        return 1;
    }

    /**
     * Transforms an Azure paragraph element.
     */
    private void transformParagraph(JsonNode paragraph, List<List<IObject>> result,
                                     Map<Integer, Double> pageHeights,
                                     Map<Integer, double[]> pageDimensions) {
        // Get role
        String role = paragraph.has("role") ? paragraph.get("role").asText() : null;

        // Skip furniture elements
        if (ROLE_PAGE_HEADER.equals(role) || ROLE_PAGE_FOOTER.equals(role)
            || ROLE_PAGE_NUMBER.equals(role)) {
            return;
        }

        // Get page number from boundingRegions
        int pageNumber = getPageNumberFromBoundingRegions(paragraph);
        int pageIndex = pageNumber - 1;

        // Ensure result list is large enough
        while (result.size() <= pageIndex) {
            result.add(new ArrayList<>());
        }

        // Get bounding box
        double pageHeight = getPageHeightInPoints(pageNumber, pageHeights, pageDimensions);
        BoundingBox bbox = extractBoundingBox(paragraph, pageIndex, pageHeight, pageDimensions.get(pageNumber));

        // Get text content
        String text = paragraph.has("content") ? paragraph.get("content").asText() : "";

        // Create appropriate IObject based on role
        IObject object;
        if (ROLE_SECTION_HEADING.equals(role)) {
            object = createHeading(text, bbox, 2);
        } else if (ROLE_TITLE.equals(role)) {
            object = createHeading(text, bbox, 1);
        } else {
            object = createParagraph(text, bbox);
        }

        if (object != null) {
            result.get(pageIndex).add(object);
        }
    }

    /**
     * Transforms an Azure table element.
     */
    private void transformTable(JsonNode tableNode, List<List<IObject>> result,
                                 Map<Integer, Double> pageHeights,
                                 Map<Integer, double[]> pageDimensions) {
        int numRows = tableNode.has("rowCount") ? tableNode.get("rowCount").asInt() : 0;
        int numCols = tableNode.has("columnCount") ? tableNode.get("columnCount").asInt() : 0;

        if (numRows == 0 || numCols == 0) {
            return;
        }

        // Get page from boundingRegions
        int pageNumber = getPageNumberFromBoundingRegions(tableNode);
        int pageIndex = pageNumber - 1;

        while (result.size() <= pageIndex) {
            result.add(new ArrayList<>());
        }

        double pageHeight = getPageHeightInPoints(pageNumber, pageHeights, pageDimensions);
        BoundingBox tableBbox = extractBoundingBox(tableNode, pageIndex, pageHeight, pageDimensions.get(pageNumber));

        // Create TableBorder
        TableBorder table = new TableBorder(numRows, numCols);
        table.setBoundingBox(tableBbox);
        table.setRecognizedStructureId(StaticLayoutContainers.incrementContentId());

        // Build cell map from Azure cells array
        Map<String, JsonNode> cellMap = new HashMap<>();
        JsonNode cells = tableNode.get("cells");
        if (cells != null && cells.isArray()) {
            for (JsonNode cell : cells) {
                int row = cell.has("rowIndex") ? cell.get("rowIndex").asInt() : 0;
                int col = cell.has("columnIndex") ? cell.get("columnIndex").asInt() : 0;
                cellMap.put(row + "," + col, cell);
            }
        }

        // Build table structure
        double rowHeight = numRows > 0 ? (tableBbox.getTopY() - tableBbox.getBottomY()) / numRows : 0;
        double colWidth = numCols > 0 ? (tableBbox.getRightX() - tableBbox.getLeftX()) / numCols : 0;

        for (int row = 0; row < numRows; row++) {
            TableBorderRow borderRow = new TableBorderRow(row, numCols, 0L);
            double rowTop = tableBbox.getTopY() - (row * rowHeight);
            double rowBottom = rowTop - rowHeight;
            borderRow.setBoundingBox(new BoundingBox(pageIndex,
                tableBbox.getLeftX(), rowBottom, tableBbox.getRightX(), rowTop));

            for (int col = 0; col < numCols; col++) {
                String key = row + "," + col;
                JsonNode cellNode = cellMap.get(key);

                int rowSpan = 1;
                int colSpan = 1;
                String cellText = "";

                if (cellNode != null) {
                    rowSpan = cellNode.has("rowSpan") ? cellNode.get("rowSpan").asInt(1) : 1;
                    colSpan = cellNode.has("columnSpan") ? cellNode.get("columnSpan").asInt(1) : 1;
                    cellText = cellNode.has("content") ? cellNode.get("content").asText() : "";
                }

                TableBorderCell cell = new TableBorderCell(row, col, rowSpan, colSpan, 0L);
                double cellLeft = tableBbox.getLeftX() + (col * colWidth);
                double cellRight = cellLeft + (colSpan * colWidth);
                double cellTop = tableBbox.getTopY() - (row * rowHeight);
                double cellBottom = cellTop - (rowSpan * rowHeight);
                cell.setBoundingBox(new BoundingBox(pageIndex, cellLeft, cellBottom, cellRight, cellTop));

                if (!cellText.isEmpty()) {
                    SemanticParagraph content = createParagraph(cellText, cell.getBoundingBox());
                    cell.addContentObject(content);
                }

                borderRow.getCells()[col] = cell;
            }

            table.getRows()[row] = borderRow;
        }

        result.get(pageIndex).add(table);
    }

    /**
     * Transforms an Azure figure element.
     */
    private void transformFigure(JsonNode figureNode, List<List<IObject>> result,
                                  Map<Integer, Double> pageHeights,
                                  Map<Integer, double[]> pageDimensions) {
        int pageNumber = getPageNumberFromBoundingRegions(figureNode);
        int pageIndex = pageNumber - 1;

        while (result.size() <= pageIndex) {
            result.add(new ArrayList<>());
        }

        double pageHeight = getPageHeightInPoints(pageNumber, pageHeights, pageDimensions);
        BoundingBox bbox = extractBoundingBox(figureNode, pageIndex, pageHeight, pageDimensions.get(pageNumber));

        SemanticPicture picture = new SemanticPicture(bbox, ++pictureIndex, null);
        picture.setRecognizedStructureId(StaticLayoutContainers.incrementContentId());

        result.get(pageIndex).add(picture);
    }

    /**
     * Transforms an Azure formula element.
     */
    private void transformFormula(JsonNode formulaNode, int pageNumber,
                                   List<List<IObject>> result,
                                   Map<Integer, Double> pageHeights,
                                   Map<Integer, double[]> pageDimensions) {
        int pageIndex = pageNumber - 1;

        while (result.size() <= pageIndex) {
            result.add(new ArrayList<>());
        }

        double pageHeight = getPageHeightInPoints(pageNumber, pageHeights, pageDimensions);

        // Azure formulas have polygon coordinates directly
        BoundingBox bbox = extractPolygonBoundingBox(formulaNode, pageIndex, pageHeight,
            pageDimensions.get(pageNumber));

        String value = formulaNode.has("value") ? formulaNode.get("value").asText() : "";

        SemanticFormula formula = new SemanticFormula(bbox, value);
        formula.setRecognizedStructureId(StaticLayoutContainers.incrementContentId());

        result.get(pageIndex).add(formula);
    }

    /**
     * Gets the page number from the boundingRegions array.
     */
    private int getPageNumberFromBoundingRegions(JsonNode node) {
        JsonNode boundingRegions = node.get("boundingRegions");
        if (boundingRegions != null && boundingRegions.isArray() && boundingRegions.size() > 0) {
            JsonNode firstRegion = boundingRegions.get(0);
            if (firstRegion.has("pageNumber")) {
                return firstRegion.get("pageNumber").asInt();
            }
        }
        return 1;
    }

    /**
     * Gets the page height in PDF points.
     */
    private double getPageHeightInPoints(int pageNumber, Map<Integer, Double> pageHeights,
                                          Map<Integer, double[]> pageDimensions) {
        // First check provided page heights (already in points)
        if (pageHeights != null && pageHeights.containsKey(pageNumber)) {
            return pageHeights.get(pageNumber);
        }
        // Compute from Azure page dimensions (in inches)
        double[] dims = pageDimensions.get(pageNumber);
        if (dims != null) {
            return dims[1] * INCHES_TO_POINTS;
        }
        // Default US Letter height
        return 11.0 * INCHES_TO_POINTS;
    }

    /**
     * Extracts a BoundingBox from an Azure element's boundingRegions polygon.
     *
     * <p>Azure uses top-left origin with coordinates in inches.
     * The polygon is [x1,y1,x2,y2,x3,y3,x4,y4] (4 corners clockwise).
     * This converts to PDF points with BOTTOMLEFT origin.
     */
    private BoundingBox extractBoundingBox(JsonNode node, int pageIndex, double pageHeight,
                                            double[] pageDims) {
        JsonNode boundingRegions = node.get("boundingRegions");
        if (boundingRegions != null && boundingRegions.isArray() && boundingRegions.size() > 0) {
            JsonNode firstRegion = boundingRegions.get(0);
            JsonNode polygon = firstRegion.get("polygon");
            if (polygon != null && polygon.isArray() && polygon.size() >= 8) {
                return polygonToBoundingBox(polygon, pageIndex, pageHeight);
            }
        }
        return new BoundingBox(pageIndex, 0, 0, 0, 0);
    }

    /**
     * Extracts a BoundingBox from an element that has a polygon directly (e.g., formulas).
     */
    private BoundingBox extractPolygonBoundingBox(JsonNode node, int pageIndex, double pageHeight,
                                                   double[] pageDims) {
        JsonNode polygon = node.get("polygon");
        if (polygon != null && polygon.isArray() && polygon.size() >= 8) {
            return polygonToBoundingBox(polygon, pageIndex, pageHeight);
        }
        return new BoundingBox(pageIndex, 0, 0, 0, 0);
    }

    /**
     * Converts an Azure polygon array to a BoundingBox.
     *
     * <p>Polygon format: [x1,y1, x2,y2, x3,y3, x4,y4]
     * where coordinates are in inches from top-left origin.
     */
    private BoundingBox polygonToBoundingBox(JsonNode polygon, int pageIndex, double pageHeight) {
        double minX = Double.MAX_VALUE;
        double maxX = Double.MIN_VALUE;
        double minY = Double.MAX_VALUE;
        double maxY = Double.MIN_VALUE;

        for (int i = 0; i < polygon.size(); i += 2) {
            double x = polygon.get(i).asDouble() * INCHES_TO_POINTS;
            double y = polygon.get(i + 1).asDouble() * INCHES_TO_POINTS;
            minX = Math.min(minX, x);
            maxX = Math.max(maxX, x);
            minY = Math.min(minY, y);
            maxY = Math.max(maxY, y);
        }

        // Convert from TOPLEFT to BOTTOMLEFT origin
        double left = minX;
        double right = maxX;
        double top = pageHeight - minY;
        double bottom = pageHeight - maxY;

        return new BoundingBox(pageIndex, left, bottom, right, top);
    }

    /**
     * Creates a SemanticParagraph.
     */
    private SemanticParagraph createParagraph(String text, BoundingBox bbox) {
        TextChunk textChunk = new TextChunk(bbox, text, 12.0, 12.0);
        textChunk.adjustSymbolEndsToBoundingBox(null);
        TextLine textLine = new TextLine(textChunk);

        SemanticParagraph paragraph = new SemanticParagraph();
        paragraph.add(textLine);
        paragraph.setRecognizedStructureId(StaticLayoutContainers.incrementContentId());
        paragraph.setCorrectSemanticScore(1.0);

        return paragraph;
    }

    /**
     * Creates a SemanticHeading.
     */
    private SemanticHeading createHeading(String text, BoundingBox bbox, int level) {
        TextChunk textChunk = new TextChunk(bbox, text, 12.0, 12.0);
        textChunk.adjustSymbolEndsToBoundingBox(null);
        TextLine textLine = new TextLine(textChunk);

        SemanticHeading heading = new SemanticHeading();
        heading.add(textLine);
        heading.setRecognizedStructureId(StaticLayoutContainers.incrementContentId());
        heading.setHeadingLevel(level);
        heading.setCorrectSemanticScore(1.0);

        return heading;
    }

    /**
     * Sorts page contents by reading order (top to bottom, left to right).
     */
    private void sortByReadingOrder(List<IObject> contents) {
        contents.sort(new Comparator<IObject>() {
            @Override
            public int compare(IObject o1, IObject o2) {
                double topDiff = o2.getTopY() - o1.getTopY();
                if (Math.abs(topDiff) > 5.0) {
                    return topDiff > 0 ? 1 : -1;
                }
                return Double.compare(o1.getLeftX(), o2.getLeftX());
            }
        });
    }
}
