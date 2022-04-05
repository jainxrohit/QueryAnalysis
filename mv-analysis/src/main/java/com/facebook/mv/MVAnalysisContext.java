package com.facebook.mv;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class MVAnalysisContext
{
    public String queryId;
    public List<String> selectFields;
    public Set<String> candidateFields;
    public String tableName;
    public BufferedWriter errorsWriter;
    public BufferedWriter resultWriter;
}
