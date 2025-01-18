package com.dataframe.converter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dataframe.parser.DataFrameAPICodeParser;
import com.dataframe.parser.DataFrameNode;

public class DataFrameCodeExtractor {
    private final DataFrameAPICodeParser parser;
    private final DataFrameToSQLConverter converter;

    public DataFrameCodeExtractor() {
        this.parser = new DataFrameAPICodeParser();
        this.converter = new DataFrameToSQLConverter();
    }

    public List<String> processFile(String filePath) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
        List<String> operations = extractDataFrameOperations(content);
        System.out.println("Operations : " + operations);
        return convertOperationsToSQL(operations);
    }

    private List<String> extractDataFrameOperations(String content) {
        List<String> operations = new ArrayList<>();
        Pattern pattern = Pattern.compile(
            "val\\s+(\\w+)\\s*=\\s*(\\w+)\\.(.*?)(?=\\s*val\\s+\\w+\\s*=|\\Z)", 
            Pattern.DOTALL | Pattern.MULTILINE
        );
        Matcher matcher = pattern.matcher(content);
        while (matcher.find()) {
            String operation = matcher.group(2) + "." + matcher.group(3).trim();
            operations.add(operation);
        }
        return operations;
    }

    private List<String> convertOperationsToSQL(List<String> operations) {
        List<String> sqlQueries = new ArrayList<>();
        for (String operation : operations) {
            DataFrameNode parsedNode = parser.parse(operation);
            if (parsedNode != null) {
                String tableName = extractTableName(operation);
                String sql = converter.convert(parsedNode, tableName);
                sqlQueries.add(sql);
            }
        }
        return sqlQueries;
    }

    private String extractTableName(String operation) {
        if (operation.contains("myDf")) {
            return "sales";
        }
        return "default_table";
    }

    public static void main(String[] args) throws IOException {
        DataFrameCodeExtractor extractor = new DataFrameCodeExtractor();
        String filePath = "/Users/saboor/Documents/Projects/Codes/Spark2SQL/SparkDataFrameExample.scala";
        List<String> sqlQueries = extractor.processFile(filePath);
        
        System.out.println("Generated SQL Queries:");
        for (String sql : sqlQueries) {
            System.out.println(sql);
            System.out.println("-------------------");
        }
    }
}