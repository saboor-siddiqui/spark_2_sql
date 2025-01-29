package com.dataframe.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataFrameNode {
    private final String type;
    private final Map<String, Object> details;
    private final DataFrameNode parent;
    private final List<DataFrameNode> children;

    public DataFrameNode(String type, Map<String, Object> details, DataFrameNode parent) {
        this.type = type;
        this.details = details;
        this.parent = parent;
        this.children = new ArrayList<>();
        if (parent != null) {
            parent.addChild(this);
        }
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getDetails() {
        return details;
    }

    public DataFrameNode getParent() {
        return parent;
    }

    public List<DataFrameNode> getChildren() {
        return children;
    }

    public void addChild(DataFrameNode child) {
        children.add(child);
    }
}
