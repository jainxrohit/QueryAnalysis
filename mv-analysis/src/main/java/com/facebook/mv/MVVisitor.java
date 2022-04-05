package com.facebook.mv;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

import static com.facebook.mv.MVAnalysis.DELIMITER;

public class MVVisitor
        extends DefaultTraversalVisitor<Void, MVAnalysisContext>
{
    boolean isTableVisited = false;
    boolean isGroupByVisited = false;

    public MVVisitor()
    {
    }

    public void extract(Node node, String queryId)
    {
        MVAnalysisContext mvAnalysisContext = new MVAnalysisContext();
        mvAnalysisContext.queryId = queryId;
        mvAnalysisContext.queryInfoBuilder = new StringBuilder();
        mvAnalysisContext.queryInfoBuilder.append(mvAnalysisContext.queryId).append(DELIMITER);
        this.process(node, mvAnalysisContext);
        System.out.println(mvAnalysisContext.queryInfoBuilder);
    }

    @Override
    protected Void visitQuery(Query node, MVAnalysisContext context)
    {
        if (node.getLimit().isPresent()) {
            throw new RuntimeException("Found Limit for query: " + context.queryId);
        }
        return super.visitQuery(node, context);
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, MVAnalysisContext context)
    {
        if (node.getLimit().isPresent()) {
            throw new RuntimeException("Found Limit for query: " + context.queryId);
        }

        if(context.queryId.equalsIgnoreCase("20220329_180035_72310_vac2i")) {
            int i=1;
        }
        Optional<Expression> where = node.getWhere();
        if(where.isPresent()) {
            context.whereFields = new TreeSet<>();
            Expression expression = where.get();
            process(expression, context);
            context.queryInfoBuilder.append("__WHERE__");
            for (String f : context.whereFields) {
                context.queryInfoBuilder.append(f).append(DELIMITER);
            }

            context.queryInfoBuilder.append("__WHERE__");

        }
        return super.visitQuerySpecification(node, context);
    }

    @Override
    protected Void visitIdentifier(Identifier node, MVAnalysisContext context) {
        context.whereFields.add(node.getValue());
        return super.visitIdentifier(node, context);
    }


    @Override
    protected Void visitFunctionCall(FunctionCall node, MVAnalysisContext context) {
        return super.visitFunctionCall(node, context);
    }

    @Override
    protected Void visitSubqueryExpression(SubqueryExpression node, MVAnalysisContext context)
    {
        throw new RuntimeException("Found Subqueries for query: " + context.queryId);
    }

    @Override
    protected Void visitOrderBy(OrderBy node, MVAnalysisContext context)
    {
        throw new RuntimeException("Found Order By for query: " + context.queryId);
    }

    public Void visitTable(Table node, MVAnalysisContext context)
    {
        if (context.queryId.equalsIgnoreCase("20220330_053030_00649_txytk")) {
            int i = 1;
        }

        if (isTableVisited) {
            throw new RuntimeException("Multiple tables found!! Skipping query: " + context.queryId);
        }
        isTableVisited = true;
        context.tableName = node.getName().toString();
        context.queryInfoBuilder.append(context.tableName).append(DELIMITER);
        if (context.tableToSelects.containsKey(context.tableName)) {
            System.err.printf("Multiple visit for the same table: %s, for the query: %s\n", context.tableName, context.queryId);
        }
        else {
            context.tableToSelects.put(context.tableName, context.selectFields);
        }

        return super.visitTable(node, context);
    }

    @Override
    protected Void visitSelect(Select select, MVAnalysisContext context)
    {
        if (context.queryId.equalsIgnoreCase("20220330_053030_00649_txytk")) {
            int i = 0;
        }
        context.selectFields = new LinkedList<>();

        List<SelectItem> selectItems = select.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) selectItem;
                if (column.getAlias().isPresent()) {
                    context.selectFields.add(String.valueOf(column.getAlias().get()));
                }
                else {
                    context.selectFields.add(String.valueOf(column.getExpression()));
                }
            }
            else {
                context.selectFields.add(selectItem.toString());
            }
        }
        return super.visitSelect(select, context);
    }

    @Override
    protected Void visitGroupBy(GroupBy groupBy, MVAnalysisContext context)
    {
        if (context.queryId.equalsIgnoreCase("20220330_053030_00649_txytk")) {
            int i = 1;
        }

        if (isGroupByVisited) {
            throw new RuntimeException("Multiple Group By found!! Skipping query: " + context.queryId);
        }

        isGroupByVisited = true;

        context.queryInfoBuilder.append("__GROUP__");
        if (!groupBy.getGroupingElements().isEmpty()) {
            for (GroupingElement groupingElement : groupBy.getGroupingElements()) {
                List<Expression> expressions = groupingElement.getExpressions();
                for (Expression expression : expressions) {
                    if (expression instanceof LongLiteral) {
                        context.queryInfoBuilder.append(context.selectFields.get((int) ((LongLiteral) expression).getValue() - 1)).append(DELIMITER);
                    }
                    else {
                        context.queryInfoBuilder.append(expression).append(DELIMITER);
                    }
                }
            }
        }
        context.queryInfoBuilder.append("__GROUP__");

        return super.visitGroupBy(groupBy, context);
    }
}
