package com.facebook.mv;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;

import java.util.LinkedList;
import java.util.List;

public class MVVisitor
        extends DefaultTraversalVisitor<Void, MVAnalysisContext>
{
    boolean isTableVisited = false;

    public MVVisitor()
    {
    }

    public void extract(Node node, String queryId)
    {
        MVAnalysisContext mvAnalysisContext = new MVAnalysisContext();
        mvAnalysisContext.queryId = queryId;
        StringBuilder sb = new StringBuilder();
        this.process(node, mvAnalysisContext);
    }

    @Override
    protected Void visitQuery(Query node, MVAnalysisContext context)
    {
        if (node.getLimit().isPresent()) {
            System.err.println("Limits are not supported");
            return null;
        }
        return super.visitQuery(node, context);
    }

    public Void visitTable(Table node, MVAnalysisContext context)
    {
        if(context.queryId.equalsIgnoreCase("20220330_053030_00649_txytk")) {
            int i=1;
        }

        if (isTableVisited) {
            throw new RuntimeException("Failed Multiple tables found!! Skipping query: " + context.queryId);
        }
        isTableVisited = true;
        context.tableName = node.getName().toString();
        if (context.tableToSelects.containsKey(context.tableName)) {
            System.err.printf("Multiple visit for the same table: %s, for the query: %s\n", context.tableName, context.queryId);
        }
        else {
            context.tableToSelects.put(context.tableName, context.fields);
        }

        return super.visitTable(node, context);
    }

    @Override
    protected Void visitSelect(Select select, MVAnalysisContext context)
    {
        if(context.queryId.equalsIgnoreCase("20220330_053030_00649_txytk")) {
            int i=0;
        }
        context.fields = new LinkedList<>();

        List<SelectItem> selectItems = select.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) selectItem;
                if (column.getAlias().isPresent()) {
                    context.fields.add(String.valueOf(column.getAlias().get()));
                }
                else {
                    context.fields.add(String.valueOf(column.getExpression()));
                }
            }
            else {
                context.fields.add(selectItem.toString());
            }
        }
        return super.visitSelect(select, context);
    }

    @Override
    protected Void visitGroupBy(GroupBy groupBy, MVAnalysisContext context)
    {
        if(context.queryId.equalsIgnoreCase("20220330_053030_00649_txytk")) {
            int i=1;
        }
        if (!groupBy.getGroupingElements().isEmpty()) {
            for (GroupingElement groupingElement : groupBy.getGroupingElements()) {
                List<Expression> expressions = groupingElement.getExpressions();
                for (Expression expression : expressions) {
                    if (expression instanceof LongLiteral) {
                        System.out.println(context.fields.get((int) ((LongLiteral) expression).getValue() - 1));
                    }
                    else {
                        System.out.println(expression.toString());
                    }
                }
            }
        }

        return super.visitGroupBy(groupBy, context);
    }
}
