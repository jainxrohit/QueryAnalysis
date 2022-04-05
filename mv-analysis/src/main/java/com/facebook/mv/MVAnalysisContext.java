package com.facebook.mv;

import com.facebook.presto.sql.tree.GroupingElement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MVAnalysisContext
{
    public String queryId;
    public List<String> fields;
    public final Map<String, List<String>> tableToSelects = new HashMap<>();
    public String tableName;
}
