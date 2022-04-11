package com.facebook.mv;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.Table;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

import static com.facebook.mv.MVAnalysis.DELIMITER;
import static java.lang.String.format;

public class MVVisitor
        extends DefaultTraversalVisitor<Void, Void>
{
    MVAnalysisContext context = new MVAnalysisContext();
    public MVVisitor()
    {
    }

    public void extract(Node node, String queryId, BufferedWriter resultWriter, BufferedWriter errorsWriter)
    {
        if (queryId.equalsIgnoreCase("20220328_131724_09160_dtzbu")) {
            System.out.println("found");
        }
        context.queryId = queryId;
        context.resultWriter = resultWriter;
        context.errorsWriter = errorsWriter;
        this.process(node);
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, Void c)
    {
        if (context.queryId.equalsIgnoreCase("20220328_131724_09160_dtzbu")) {
            int i = 1;
            System.out.println(i);
        }
        Optional<Relation> from = node.getFrom();
        if (from.isPresent()) {
            if (from.get() instanceof Table) {
                process(from.get(), c);
            }
            else if (from.get() instanceof AliasedRelation) {
                AliasedRelation aliasedRelation = (AliasedRelation) from.get();
                if (aliasedRelation.getRelation() instanceof Table) {
                    process(aliasedRelation.getRelation(), c);
                }
            }
            else {
                throw new RuntimeException(format("[%s] Unsupported from relation: %s", context.queryId, from.get()));
            }
        }

        Select select = node.getSelect();
        process(select, c);

        context.candidateFields = new TreeSet<>();
        Optional<Expression> where = node.getWhere();
        if (where.isPresent()) {
            Expression expression = where.get();
            process(expression, c);
        }

        if (node.getGroupBy().isPresent()) {
            GroupBy groupBy = node.getGroupBy().get();
            process(groupBy, c);
        }

        if (context.tableName != null && !context.candidateFields.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(context.queryId).append(DELIMITER);

            sb.append(context.tableName).append(DELIMITER);
            for (String candidateField : context.candidateFields) {
                sb.append(candidateField).append(DELIMITER);
            }
            sb.append('\n');
            try {
                context.resultWriter.write(sb.toString());
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to write to file");
            }
        }

        return null;
    }

    @Override
    protected Void visitIdentifier(Identifier node, Void c)
    {
        context.candidateFields.add(node.getValue());
        return null;
    }

    @Override
    protected Void visitSubqueryExpression(SubqueryExpression node, Void c)
    {
        throw new RuntimeException("Found Subqueries for query: " + context.queryId);
    }

    @Override
    protected Void visitOrderBy(OrderBy node, Void c)
    {
        throw new RuntimeException("Found Order By for query: " + context.queryId);
    }

    public Void visitTable(Table node, Void c)
    {
        context.tableName = node.getName().toString();
        return null;
    }

    @Override
    protected Void visitSelect(Select select, Void c)
    {
        context.selectFields = new LinkedList<>();
        // List<SelectItem> selectItems = select.getSelectItems();
        //            if (selectItem instanceof SingleColumn) {
        //                SingleColumn column = (SingleColumn) selectItem;
        //                if (column.getAlias().isPresent()) {
        //                    context.selectFields.add(String.valueOf(column.getAlias().get()));
        //                }
        //                else {
        //                    context.selectFields.add(String.valueOf(column.getExpression()));
        //                }
        //            }
        //            else {
        //                context.selectFields.add(selectItem.toString());
        //            }
        context.selectFields.addAll(select.getSelectItems());
        return null;
    }

    @Override
    protected Void visitGroupBy(GroupBy groupBy, Void c)
    {
        if (!groupBy.getGroupingElements().isEmpty()) {
            for (GroupingElement groupingElement : groupBy.getGroupingElements()) {
                List<Expression> expressions = groupingElement.getExpressions();
                for (Expression expression : expressions) {
                    if (expression instanceof LongLiteral) {
                        SelectItem selectItem = context.selectFields.get((int) ((LongLiteral) expression).getValue() - 1);
                        process(selectItem);
                    }
                    else if (expression instanceof Identifier) {
                        context.candidateFields.add(expression.toString());
                    }
                    else {
                        process(expression);
                    }
                }
            }
        }

        return null;
    }
}
