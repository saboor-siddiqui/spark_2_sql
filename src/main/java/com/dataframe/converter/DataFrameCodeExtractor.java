package com.dataframe.converter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataframe.parser.DataFrameAPICodeParser;
import com.dataframe.parser.DataFrameNode;

public class DataFrameCodeExtractor {
    private final DataFrameAPICodeParser parser;
    private final DataFrameToSQLConverter converter;
    private static final Logger logger = LoggerFactory.getLogger(DataFrameCodeExtractor.class);
    public DataFrameCodeExtractor() {
        this.parser = new DataFrameAPICodeParser();
        this.converter = new DataFrameToSQLConverter();
    }

    public List<String> processFile(String filePath) throws IOException {
        logger.info("Processing file: {}", filePath);
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalArgumentException("File path cannot be null or empty");
        }
        
        if (!Files.exists(Paths.get(filePath))) {
            throw new IOException("File does not exist: " + filePath);
        }
        
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
        if (content.trim().isEmpty()) {
            throw new IOException("File is empty: " + filePath);
        }
        
        logger.debug("File content read: {}", content);
        List<String> operations = extractDataFrameOperations(content);
        if (operations.isEmpty()) {
            System.out.println("Warning: No DataFrame operations found in file");
        }
        return convertOperationsToSQL(operations);
    }

    private List<String> extractDataFrameOperations(String content) {
        List<String> operations = new ArrayList<>();
        
        // Clean up the content
        content = content.replaceAll("//.*?\\n", "\n")
                        .replaceAll("/\\*.*?\\*/", "")
                        .replaceAll("(?m)^\\s+", "")
                        .trim();
        
        // Updated pattern to handle multi-line method definitions with new agg syntax
        Pattern methodPattern = Pattern.compile(
            "def\\s+(\\w+)\\s*:\\s*DataFrame\\s*=\\s*\\{([^}]+?)\\s*\\}",
            Pattern.MULTILINE | Pattern.DOTALL
        );
        
        // Extract method-style operations
        Matcher methodMatcher = methodPattern.matcher(content);
        while (methodMatcher.find()) {
            String methodBody = methodMatcher.group(2).trim();
            // Clean up the method body while preserving agg expressions
            String cleanedOperation = methodBody
                .replaceAll("\\s*\\.\\s*", ".")  // Clean up dots
                .replaceAll("(?m)^\\s+", "")     // Remove leading spaces
                .replaceAll("\\n\\s*", " ")      // Convert newlines to spaces
                .replaceAll("\\s+", " ")         // Normalize spaces
                .replaceAll("\\s*,\\s*", ", ")   // Clean up commas
                .trim();
            
            operations.add(cleanedOperation);
        }
        
        // Keep existing variable-style extraction
        Pattern varPattern = Pattern.compile(
            "val\\s+(\\w+)\\s*=\\s*(\\w+)\\.((?:[^\\n]*?\\n?\\s*\\.?)*?)(?=\\s*val|\\s*$)",
            Pattern.MULTILINE | Pattern.DOTALL
        );
        
        Matcher varMatcher = varPattern.matcher(content);
        while (varMatcher.find()) {
            String dfVariable = varMatcher.group(2);
            String operation = varMatcher.group(3);
            String cleanedOperation = operation
                .replaceAll("\\s*\\.\\s*", ".")
                .replaceAll("(?m)^\\s+", "")
                .replaceAll("\\n\\s*", "")
                .replaceAll("\\s+", " ")
                .trim();
            
            operations.add(dfVariable + "." + cleanedOperation);
        }
        
        return operations;
    }

    private String normalizeSparkCode(String operation) {
        // Handle readCS3Data calls
        operation = operation.replaceAll(
            "readCS3Data\\s*\\([^,]+,\\s*\"([^\"]+)\",\\s*\"([^\"]+)\"\\)",
            "spark.read.table(\"$1.$2\")"
        );
        
        // Handle column expressions
        operation = operation.replaceAll("col\\(\"([^\"]+)\"\\)", "$1")
                            .replaceAll("===", "=")
                            .replaceAll("\\.isNotNull", " IS NOT NULL")
                            .replaceAll("trim\\(([^)]+)\\)", "TRIM($1)");
        
        // Handle aggregations with 'as' keyword
        operation = operation.replaceAll(
            "(\\w+)\\(\"([^\"]+)\"\\)\\s+as\\s+\"([^\"]+)\"",
            "$1($2) as $3"
        );
        
        // Handle date functions
        operation = operation.replaceAll(
            "date_trunc\\(\"([^\"]+)\",\\s*([^)]+)\\)",
            "DATE_TRUNC('$1', $2)"
        );
        
        return operation;
    }

    private List<String> convertOperationsToSQL(List<String> operations) {
        List<String> sqlQueries = new ArrayList<>();
        for (String operation : operations) {
            // Normalize the Spark code before parsing
            operation = normalizeSparkCode(operation);
            System.out.println("Normalized Operation: " + operation);
            DataFrameNode parsedNode = parser.parse(operation);
            if (parsedNode != null) {
                String tableName = parser.extractTableName(operation);
                String sql = converter.convert(parsedNode, tableName);
                sqlQueries.add(sql);
            }
        }
        return sqlQueries;
    }

    public static void main(String[] args) throws IOException {
        DataFrameCodeExtractor extractor = new DataFrameCodeExtractor();
        String inputFilePath = "/Users/saboor/Documents/Projects/Codes/Spark2SQL/SparkDataFrameExample.scala";
        String outputFilePath = "/Users/saboor/Documents/Projects/Codes/Spark2SQL/SQL_Output.sql";
        
        File outputFile = new File(outputFilePath);
        outputFile.getParentFile().mkdirs();
        
        List<String> sqlQueries = extractor.processFile(inputFilePath);
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("-- Generated SQL Queries\n");
            writer.write("-- Source: " + inputFilePath + "\n");
            writer.write("-- Generated at: " + LocalDateTime.now() + "\n\n");
            
            for (String sql : sqlQueries) {
                writer.write(sql + ";\n\n");
            }
        } catch (IOException e) {
            logger.error("Error writing to SQL file: {}", e.getMessage());
        }
    }
}