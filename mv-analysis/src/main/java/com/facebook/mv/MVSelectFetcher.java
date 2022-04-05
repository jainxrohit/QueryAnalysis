package com.facebook.mv;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;

import java.util.LinkedList;
import java.util.List;

public class MVSelectFetcher
        extends DefaultTraversalVisitor<Void, MVAnalysisContext>
{
    public MVAnalysisContext mvAnalysisContext = new MVAnalysisContext();

    public MVAnalysisContext getMvAnalysisContext()
    {
        return mvAnalysisContext;
    }

    public void extract(Node node, String queryId)
    {
        mvAnalysisContext.queryId = queryId;
        this.process(node, mvAnalysisContext);
    }

    @Override
    protected Void visitTable(Table table, MVAnalysisContext context)
    {
        if (context.queryId.equalsIgnoreCase("20220330_023023_00470_sev2c")) {
            System.out.println("Debug");
        }

        context.tableToSelects.put(table.getName().toString(), context.fields);
        return super.visitTable(table, context);
    }

    @Override
    protected Void visitSelect(Select select, MVAnalysisContext context)
    {
        if (context.queryId.equalsIgnoreCase("20220330_023023_00470_sev2c")) {
            System.out.println("Debug");
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
}
