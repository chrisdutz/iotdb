/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.table.sql.parser;

import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.NodeLocation;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Statement;
import org.apache.iotdb.db.relational.grammar.sql.TableSqlBaseListener;
import org.apache.iotdb.db.relational.grammar.sql.TableSqlLexer;
import org.apache.iotdb.db.relational.grammar.sql.TableSqlParser;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SqlParser {
  private static final ANTLRErrorListener LEXER_ERROR_LISTENER =
      new BaseErrorListener() {
        @Override
        public void syntaxError(
            Recognizer<?, ?> recognizer,
            Object offendingSymbol,
            int line,
            int charPositionInLine,
            String message,
            RecognitionException e) {
          throw new ParsingException(message, e, line, charPositionInLine + 1);
        }
      };
  private static final BiConsumer<TableSqlLexer, TableSqlParser> DEFAULT_PARSER_INITIALIZER =
      (TableSqlLexer lexer, TableSqlParser parser) -> {};

  private static final ErrorHandler PARSER_ERROR_HANDLER =
      ErrorHandler.builder()
          .specialRule(TableSqlParser.RULE_expression, "<expression>")
          .specialRule(TableSqlParser.RULE_booleanExpression, "<expression>")
          .specialRule(TableSqlParser.RULE_valueExpression, "<expression>")
          .specialRule(TableSqlParser.RULE_primaryExpression, "<expression>")
          .specialRule(TableSqlParser.RULE_predicate, "<predicate>")
          .specialRule(TableSqlParser.RULE_identifier, "<identifier>")
          .specialRule(TableSqlParser.RULE_string, "<string>")
          .specialRule(TableSqlParser.RULE_query, "<query>")
          .specialRule(TableSqlParser.RULE_type, "<type>")
          .specialToken(TableSqlParser.INTEGER_VALUE, "<integer>")
          .build();

  private final BiConsumer<TableSqlLexer, TableSqlParser> initializer;

  public SqlParser() {
    this(DEFAULT_PARSER_INITIALIZER);
  }

  public SqlParser(BiConsumer<TableSqlLexer, TableSqlParser> initializer) {
    this.initializer = requireNonNull(initializer, "initializer is null");
  }

  public Statement createStatement(String sql, ZoneId zoneId) {
    return (Statement) invokeParser("statement", sql, TableSqlParser::singleStatement, zoneId);
  }

  public Statement createStatement(String sql, NodeLocation location, ZoneId zoneId) {
    return (Statement)
        invokeParser(
            "statement",
            sql,
            Optional.ofNullable(location),
            TableSqlParser::singleStatement,
            zoneId);
  }

  public Expression createExpression(String expression, ZoneId zoneId) {
    return (Expression)
        invokeParser("expression", expression, TableSqlParser::standaloneExpression, zoneId);
  }

  public DataType createType(String expression, ZoneId zoneId) {
    return (DataType) invokeParser("type", expression, TableSqlParser::standaloneType, zoneId);
  }

  private Node invokeParser(
      String name,
      String sql,
      Function<TableSqlParser, ParserRuleContext> parseFunction,
      ZoneId zoneId) {
    return invokeParser(name, sql, Optional.empty(), parseFunction, zoneId);
  }

  private Node invokeParser(
      String name,
      String sql,
      Optional<NodeLocation> location,
      Function<TableSqlParser, ParserRuleContext> parseFunction,
      ZoneId zoneId) {
    try {
      TableSqlLexer lexer =
          new TableSqlLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
      CommonTokenStream tokenStream = new CommonTokenStream(lexer);
      TableSqlParser parser = new TableSqlParser(tokenStream);
      initializer.accept(lexer, parser);

      // Override the default error strategy to not attempt inserting or deleting a token.
      // Otherwise, it messes up error reporting
      parser.setErrorHandler(
          new DefaultErrorStrategy() {
            @Override
            public Token recoverInline(Parser recognizer) throws RecognitionException {
              if (nextTokensContext == null) {
                throw new InputMismatchException(recognizer);
              }
              throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
            }
          });

      parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames()), parser));

      lexer.removeErrorListeners();
      lexer.addErrorListener(LEXER_ERROR_LISTENER);

      parser.removeErrorListeners();
      parser.addErrorListener(PARSER_ERROR_HANDLER);

      ParserRuleContext tree;
      try {
        try {
          // first, try parsing with potentially faster SLL mode
          parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
          tree = parseFunction.apply(parser);
        } catch (ParsingException ex) {
          // if we fail, parse with LL mode
          tokenStream.seek(0); // rewind input stream
          parser.reset();

          parser.getInterpreter().setPredictionMode(PredictionMode.LL);
          tree = parseFunction.apply(parser);
        }
      } catch (ParsingException e) {
        location.ifPresent(
            statementLocation -> {
              int line = statementLocation.getLineNumber();
              int column = statementLocation.getColumnNumber();
              throw new ParsingException(
                  e.getErrorMessage(),
                  (RecognitionException) e.getCause(),
                  e.getLineNumber() + line - 1,
                  e.getColumnNumber() + (line == 1 ? column : 0));
            });
        throw e;
      }

      return new AstBuilder(location.orElse(null), zoneId).visit(tree);
    } catch (StackOverflowError e) {
      throw new ParsingException(name + " is too large (stack overflow while parsing)");
    }
  }

  private static class PostProcessor extends TableSqlBaseListener {
    private final List<String> ruleNames;
    private final TableSqlParser parser;

    public PostProcessor(List<String> ruleNames, TableSqlParser parser) {
      this.ruleNames = ruleNames;
      this.parser = parser;
    }

    @Override
    public void exitQuotedIdentifier(TableSqlParser.QuotedIdentifierContext context) {
      Token token = context.QUOTED_IDENTIFIER().getSymbol();
      if (token.getText().length() == 2) { // empty identifier
        throw new ParsingException(
            "Zero-length delimited identifier not allowed",
            null,
            token.getLine(),
            token.getCharPositionInLine() + 1);
      }
    }

    @Override
    public void exitBackQuotedIdentifier(TableSqlParser.BackQuotedIdentifierContext context) {
      Token token = context.BACKQUOTED_IDENTIFIER().getSymbol();
      throw new ParsingException(
          "backquoted identifiers are not supported; use double quotes to quote identifiers",
          null,
          token.getLine(),
          token.getCharPositionInLine() + 1);
    }

    //    @Override
    //    public void exitDigitIdentifier(TableSqlParser.DigitIdentifierContext context) {
    //      Token token = context.DIGIT_IDENTIFIER().getSymbol();
    //      throw new ParsingException(
    //          "identifiers must not start with a digit; surround the identifier with double
    // quotes",
    //          null,
    //          token.getLine(),
    //          token.getCharPositionInLine() + 1);
    //    }

    @Override
    public void exitNonReserved(TableSqlParser.NonReservedContext context) {
      // we can't modify the tree during rule enter/exit event handling unless we're dealing with a
      // terminal.
      // Otherwise, ANTLR gets confused and fires spurious notifications.
      if (!(context.getChild(0) instanceof TerminalNode)) {
        int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
        throw new AssertionError(
            "nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
      }

      // replace nonReserved words with IDENT tokens
      context.getParent().removeLastChild();

      Token token = (Token) context.getChild(0).getPayload();
      Token newToken =
          new CommonToken(
              new Pair<>(token.getTokenSource(), token.getInputStream()),
              TableSqlLexer.IDENTIFIER,
              token.getChannel(),
              token.getStartIndex(),
              token.getStopIndex());

      context.getParent().addChild(parser.createTerminalNode(context.getParent(), newToken));
    }
  }
}
