package com.facebook.mv;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class MVAnalysisContext
{
    public String queryId;
    public List<String> selectFields;
    public Set<String> whereFields;
    public final Map<String, List<String>> tableToSelects = new HashMap<>();
    public String tableName;
    public StringBuilder queryInfoBuilder;
}
