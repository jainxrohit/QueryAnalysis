package com.facebook.mv;

import com.facebook.presto.sql.tree.SelectItem;

import java.io.BufferedWriter;
import java.util.List;
import java.util.Set;

public final class MVAnalysisContext
{
    public String queryId;
    public List<SelectItem> selectFields;
    public Set<String> candidateFields;
    public String tableName;
    public BufferedWriter errorsWriter;
    public BufferedWriter resultWriter;
}
