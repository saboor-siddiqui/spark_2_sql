package com.dataframe.converter;

import com.dataframe.parser.DataFrameAPICodeParser;
import com.dataframe.parser.DataFrameNode;

/**
 * Command-line entry point for the Python bridge.
 *
 * Usage:
 *   java -cp <jar> com.dataframe.converter.BridgeCLI "<chain>" "<tablePrefix>"
 *
 * Prints exactly one line:
 *   Generated SQL: <sql>
 * or exits with code 1 on failure.
 */
public class BridgeCLI {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: BridgeCLI \"<chain>\" [\"<tablePrefix>\"]");
            System.exit(1);
        }

        String chain       = args[0];
        String tablePrefix = args.length >= 2 ? args[1] : "";

        DataFrameAPICodeParser parser    = new DataFrameAPICodeParser(tablePrefix);
        DataFrameToSQLConverter converter = new DataFrameToSQLConverter();

        DataFrameNode root = parser.parse(chain);
        if (root == null) {
            System.err.println("Parse failed for chain: " + chain);
            System.exit(1);
        }

        String tableName = parser.extractTableName(chain);
        String sql       = converter.convert(root, tableName);

        System.out.println("Generated SQL: " + sql);
    }
}
