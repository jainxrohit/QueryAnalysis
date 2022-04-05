package com.facebook.mv;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.tree.Statement;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static com.facebook.presto.sql.parser.IdentifierSymbol.AT_SIGN;
import static com.facebook.presto.sql.parser.IdentifierSymbol.COLON;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

public class MVAnalysis
{
    public static String FILE_NAME = "/Users/rohitism/work/prestosql/mv_analysis/unidash_simple_20.csv";
    public static String DELIMITER = "QUERY_END_STR->";
    public static final SqlParser sqlParser = new SqlParser(new SqlParserOptions().allowIdentifierSymbol(AT_SIGN, COLON));
    public static ParsingOptions parsingOptions = ParsingOptions.builder().setDecimalLiteralTreatment(AS_DECIMAL).build();

    public static void main(String[] args)
    {
        MVAnalysis mvAnalysis = new MVAnalysis();
        mvAnalysis.process();
    }

    private void process()
    {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(FILE_NAME));
            StringBuilder queryBuilder = new StringBuilder();
            String line = reader.readLine();
            while (line != null) {
                int index = line.indexOf(DELIMITER);
                if (index != -1) {
                    queryBuilder.append(line, 0, index);
                    String queryId = line.substring(index + DELIMITER.length(), line.length() - 1);
                    parseQuery(queryId, queryBuilder.toString().replaceAll("\"\"", "\"").substring(1));
                    queryBuilder.setLength(0);
                }
                else {
                    queryBuilder.append(line).append('\n');
                }
                line = reader.readLine();
            }
            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void parseQuery(String queryId, String query)
    {
        try {
            System.out.println("ID=> " + queryId);
            Statement statement = sqlParser.createStatement(query, parsingOptions);
            MVVisitor mvVisitor = new MVVisitor();
            mvVisitor.extract(statement, queryId);
        }
        catch (Exception ex) {
            System.err.printf("Failed to parse query(%s), with following exception: (%s)%n", queryId, ex);
        }
    }
}
