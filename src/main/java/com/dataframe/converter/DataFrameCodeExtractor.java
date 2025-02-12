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
        
        // 1. First normalize the content to handle multi-line statements
        content = content.replaceAll("//.*?\\n", "\n")  // Remove single-line comments
                        .replaceAll("/\\*.*?\\*/", "")   // Remove multi-line comments
                        .replaceAll("(?m)^\\s+", "")     // Remove leading whitespace
                        .trim();
        
        // 2. Updated regex pattern to capture complete DataFrame chains
        Pattern pattern = Pattern.compile(
            "val\\s+(\\w+)\\s*=\\s*(\\w+)\\.((?:[^\\n]*?\\n?\\s*\\.?)*?)(?=\\s*val|\\s*$)",
            Pattern.MULTILINE | Pattern.DOTALL
        );
        
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            String dfVariable = matcher.group(2);
            String operation = matcher.group(3);
            
            // 3. Clean up the operation string more thoroughly
            String cleanedOperation = operation
                .replaceAll("\\s*\\.\\s*", ".")
                .replaceAll("(?m)^\\s+", "")
                .replaceAll("\\n\\s*", "")
                .replaceAll("\\s+", " ")
                .replaceAll("\\s*=\\s*", "=")
                .trim();
            
            operations.add(dfVariable + "." + cleanedOperation);
        }
        
        return operations;
    }

    private List<String> convertOperationsToSQL(List<String> operations) {
        List<String> sqlQueries = new ArrayList<>();
        for (String operation : operations) {
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