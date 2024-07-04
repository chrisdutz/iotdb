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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.AddColumn;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.AllColumns;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.AllRows;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ColumnDefinition;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.CreateDB;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.CreateIndex;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.CreateTable;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.CurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.CurrentTime;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.CurrentUser;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DataType;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DataTypeParameter;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DereferenceExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DescribeTable;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DropColumn;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DropDB;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DropIndex;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.DropTable;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ExistsPredicate;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.GenericDataType;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.GroupBy;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.GroupByTime;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.GroupingElement;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.GroupingSets;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.JoinCriteria;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.JoinOn;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.JoinUsing;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Limit;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.NaturalJoin;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.NodeLocation;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.NumericParameter;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Offset;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.OrderBy;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Parameter;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Property;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.QueryBody;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Relation;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.RenameColumn;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.RenameTable;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Row;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Select;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SelectItem;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SetProperties;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ShowCluster;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ShowConfigNodes;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ShowDB;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ShowDataNodes;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ShowIndex;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ShowRegions;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.ShowTables;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SimpleGroupBy;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SingleColumn;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SortItem;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.TimeRange;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.TypeParameter;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.UpdateAssignment;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Use;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Values;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.WhenClause;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.With;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.WithQuery;
import org.apache.iotdb.db.relational.grammar.sql.TableSqlBaseVisitor;
import org.apache.iotdb.db.relational.grammar.sql.TableSqlLexer;
import org.apache.iotdb.db.relational.grammar.sql.TableSqlParser;
import org.apache.iotdb.db.utils.DateTimeUtils;

import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tsfile.utils.TimeDuration;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ID;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.MEASUREMENT;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TIME;
import static org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor.parseDateTimeFormat;
import static org.apache.iotdb.db.queryengine.plan.table.sql.ast.GroupingSets.Type.CUBE;
import static org.apache.iotdb.db.queryengine.plan.table.sql.ast.GroupingSets.Type.EXPLICIT;
import static org.apache.iotdb.db.queryengine.plan.table.sql.ast.GroupingSets.Type.ROLLUP;

public class AstBuilder extends TableSqlBaseVisitor<Node> {

  private int parameterPosition;

  @Nullable private final NodeLocation baseLocation;

  private final ZoneId zoneId;

  AstBuilder(@Nullable NodeLocation baseLocation, ZoneId zoneId) {
    this.baseLocation = baseLocation;
    this.zoneId = zoneId;
  }

  @Override
  public Node visitSingleStatement(TableSqlParser.SingleStatementContext ctx) {
    return visit(ctx.statement());
  }

  @Override
  public Node visitStandaloneExpression(TableSqlParser.StandaloneExpressionContext context) {
    return visit(context.expression());
  }

  @Override
  public Node visitStandaloneType(TableSqlParser.StandaloneTypeContext context) {
    return visit(context.type());
  }

  // ******************* statements **********************
  @Override
  public Node visitUseDatabaseStatement(TableSqlParser.UseDatabaseStatementContext ctx) {
    return new Use(getLocation(ctx), (Identifier) visit(ctx.database));
  }

  @Override
  public Node visitShowDatabasesStatement(TableSqlParser.ShowDatabasesStatementContext ctx) {
    return new ShowDB(getLocation(ctx));
  }

  @Override
  public Node visitCreateDbStatement(TableSqlParser.CreateDbStatementContext ctx) {
    List<Property> properties = ImmutableList.of();
    if (ctx.properties() != null) {
      properties = visit(ctx.properties().propertyAssignments().property(), Property.class);
    }

    return new CreateDB(
        getLocation(ctx),
        ctx.EXISTS() != null,
        ((Identifier) visit(ctx.database)).getValue(),
        properties);
  }

  @Override
  public Node visitDropDbStatement(TableSqlParser.DropDbStatementContext ctx) {
    return new DropDB(getLocation(ctx), (Identifier) visit(ctx.database), ctx.EXISTS() != null);
  }

  @Override
  public Node visitCreateTableStatement(TableSqlParser.CreateTableStatementContext ctx) {
    List<Property> properties = ImmutableList.of();
    if (ctx.properties() != null) {
      properties = visit(ctx.properties().propertyAssignments().property(), Property.class);
    }
    return new CreateTable(
        getLocation(ctx),
        getQualifiedName(ctx.qualifiedName()),
        visit(ctx.columnDefinition(), ColumnDefinition.class),
        ctx.EXISTS() != null,
        ctx.charsetDesc() == null
            ? null
            : ((Identifier) visit(ctx.charsetDesc().identifierOrString())).getValue(),
        properties);
  }

  @Override
  public Node visitColumnDefinition(TableSqlParser.ColumnDefinitionContext ctx) {
    return new ColumnDefinition(
        getLocation(ctx),
        (Identifier) visit(ctx.identifier()),
        (DataType) visit(ctx.type()),
        getColumnCategory(ctx.columnCategory),
        ctx.charsetName() == null
            ? null
            : ((Identifier) visit(ctx.charsetName().identifier())).getValue());
  }

  @Override
  public Node visitDropTableStatement(TableSqlParser.DropTableStatementContext ctx) {
    return new DropTable(
        getLocation(ctx), getQualifiedName(ctx.qualifiedName()), ctx.EXISTS() != null);
  }

  @Override
  public Node visitShowTableStatement(TableSqlParser.ShowTableStatementContext ctx) {
    if (ctx.database == null) {
      return new ShowTables(getLocation(ctx));
    } else {
      return new ShowTables(getLocation(ctx), (Identifier) visit(ctx.database));
    }
  }

  @Override
  public Node visitDescTableStatement(TableSqlParser.DescTableStatementContext ctx) {
    return new DescribeTable(getLocation(ctx), getQualifiedName(ctx.table));
  }

  @Override
  public Node visitRenameTable(TableSqlParser.RenameTableContext ctx) {
    return new RenameTable(
        getLocation(ctx), getQualifiedName(ctx.from), (Identifier) visit(ctx.to));
  }

  @Override
  public Node visitAddColumn(TableSqlParser.AddColumnContext ctx) {
    return new AddColumn(getQualifiedName(ctx.tableName), (ColumnDefinition) visit(ctx.column));
  }

  @Override
  public Node visitRenameColumn(TableSqlParser.RenameColumnContext ctx) {
    return new RenameColumn(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        (Identifier) visit(ctx.from),
        (Identifier) visit(ctx.to));
  }

  @Override
  public Node visitDropColumn(TableSqlParser.DropColumnContext ctx) {
    return new DropColumn(
        getLocation(ctx), getQualifiedName(ctx.tableName), (Identifier) visit(ctx.column));
  }

  @Override
  public Node visitSetTableProperties(TableSqlParser.SetTablePropertiesContext ctx) {
    List<Property> properties = ImmutableList.of();
    if (ctx.propertyAssignments() != null) {
      properties = visit(ctx.propertyAssignments().property(), Property.class);
    }
    return new SetProperties(
        getLocation(ctx),
        SetProperties.Type.TABLE,
        getQualifiedName(ctx.qualifiedName()),
        properties);
  }

  @Override
  public Node visitCreateIndexStatement(TableSqlParser.CreateIndexStatementContext ctx) {
    return new CreateIndex(
        getLocation(ctx),
        getQualifiedName(ctx.tableName),
        (Identifier) visit(ctx.indexName),
        visit(ctx.identifierList().identifier(), Identifier.class));
  }

  @Override
  public Node visitDropIndexStatement(TableSqlParser.DropIndexStatementContext ctx) {
    return new DropIndex(
        getLocation(ctx), getQualifiedName(ctx.tableName), (Identifier) visit(ctx.indexName));
  }

  @Override
  public Node visitShowIndexStatement(TableSqlParser.ShowIndexStatementContext ctx) {
    return new ShowIndex(getLocation(ctx), getQualifiedName(ctx.tableName));
  }

  @Override
  public Node visitInsertStatement(TableSqlParser.InsertStatementContext ctx) {
    if (ctx.columnAliases() != null) {
      return new Insert(
          new Table(getQualifiedName(ctx.tableName)),
          visit(ctx.columnAliases().identifier(), Identifier.class),
          (Query) visit(ctx.query()));
    } else {
      return new Insert(new Table(getQualifiedName(ctx.tableName)), (Query) visit(ctx.query()));
    }
  }

  @Override
  public Node visitDeleteStatement(TableSqlParser.DeleteStatementContext ctx) {
    if (ctx.booleanExpression() != null) {
      return new Delete(
          getLocation(ctx),
          new Table(getLocation(ctx), getQualifiedName(ctx.tableName)),
          (Expression) visit(ctx.booleanExpression()));
    } else {
      return new Delete(
          getLocation(ctx), new Table(getLocation(ctx), getQualifiedName(ctx.tableName)));
    }
  }

  @Override
  public Node visitUpdateStatement(TableSqlParser.UpdateStatementContext ctx) {
    if (ctx.booleanExpression() != null) {
      return new Update(
          getLocation(ctx),
          new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
          visit(ctx.updateAssignment(), UpdateAssignment.class),
          (Expression) visit(ctx.booleanExpression()));
    } else {
      return new Update(
          getLocation(ctx),
          new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName())),
          visit(ctx.updateAssignment(), UpdateAssignment.class));
    }
  }

  @Override
  public Node visitUpdateAssignment(TableSqlParser.UpdateAssignmentContext ctx) {
    return new UpdateAssignment(
        (Identifier) visit(ctx.identifier()), (Expression) visit(ctx.expression()));
  }

  @Override
  public Node visitCreateFunctionStatement(TableSqlParser.CreateFunctionStatementContext ctx) {
    return super.visitCreateFunctionStatement(ctx);
  }

  @Override
  public Node visitUriClause(TableSqlParser.UriClauseContext ctx) {
    return super.visitUriClause(ctx);
  }

  @Override
  public Node visitDropFunctionStatement(TableSqlParser.DropFunctionStatementContext ctx) {
    return super.visitDropFunctionStatement(ctx);
  }

  @Override
  public Node visitShowFunctionsStatement(TableSqlParser.ShowFunctionsStatementContext ctx) {
    return super.visitShowFunctionsStatement(ctx);
  }

  @Override
  public Node visitLoadTsFileStatement(TableSqlParser.LoadTsFileStatementContext ctx) {
    return super.visitLoadTsFileStatement(ctx);
  }

  @Override
  public Node visitShowDevicesStatement(TableSqlParser.ShowDevicesStatementContext ctx) {
    return super.visitShowDevicesStatement(ctx);
  }

  @Override
  public Node visitCountDevicesStatement(TableSqlParser.CountDevicesStatementContext ctx) {
    return super.visitCountDevicesStatement(ctx);
  }

  @Override
  public Node visitShowClusterStatement(TableSqlParser.ShowClusterStatementContext ctx) {
    boolean details = ctx.DETAILS() != null;
    return new ShowCluster(details);
  }

  @Override
  public Node visitShowRegionsStatement(TableSqlParser.ShowRegionsStatementContext ctx) {
    TConsensusGroupType regionType = null;
    if (ctx.DATA() != null) {
      regionType = TConsensusGroupType.DataRegion;
    } else if (ctx.SCHEMA() != null) {
      regionType = TConsensusGroupType.SchemaRegion;
    }
    List<PartialPath> storageGroups = new ArrayList<>();
    if (ctx.identifier() != null) {
      try {
        // When using the table model, only single level databases are allowed to be used.
        // Therefore, the "root." prefix is omitted from the query syntax, but we need to
        // add it back before querying the server.
        storageGroups.add(new PartialPath("root." + ctx.identifier().getText()));
      } catch (IllegalPathException e) {
        throw new RuntimeException(e);
      }
    }
    // TODO: This will be left untouched for now, well add filtering later on.
    List<Integer> nodeIds = new ArrayList<>();
    return new ShowRegions(regionType, storageGroups, nodeIds);
  }

  @Override
  public Node visitShowDataNodesStatement(TableSqlParser.ShowDataNodesStatementContext ctx) {
    return new ShowDataNodes();
  }

  @Override
  public Node visitShowConfigNodesStatement(TableSqlParser.ShowConfigNodesStatementContext ctx) {
    return new ShowConfigNodes();
  }

  @Override
  public Node visitShowClusterIdStatement(TableSqlParser.ShowClusterIdStatementContext ctx) {
    return super.visitShowClusterIdStatement(ctx);
  }

  @Override
  public Node visitShowRegionIdStatement(TableSqlParser.ShowRegionIdStatementContext ctx) {
    return super.visitShowRegionIdStatement(ctx);
  }

  @Override
  public Node visitShowTimeSlotListStatement(TableSqlParser.ShowTimeSlotListStatementContext ctx) {
    return super.visitShowTimeSlotListStatement(ctx);
  }

  @Override
  public Node visitCountTimeSlotListStatement(
      TableSqlParser.CountTimeSlotListStatementContext ctx) {
    return super.visitCountTimeSlotListStatement(ctx);
  }

  @Override
  public Node visitShowSeriesSlotListStatement(
      TableSqlParser.ShowSeriesSlotListStatementContext ctx) {
    return super.visitShowSeriesSlotListStatement(ctx);
  }

  @Override
  public Node visitMigrateRegionStatement(TableSqlParser.MigrateRegionStatementContext ctx) {
    return super.visitMigrateRegionStatement(ctx);
  }

  @Override
  public Node visitShowVariablesStatement(TableSqlParser.ShowVariablesStatementContext ctx) {
    return super.visitShowVariablesStatement(ctx);
  }

  @Override
  public Node visitFlushStatement(TableSqlParser.FlushStatementContext ctx) {
    return super.visitFlushStatement(ctx);
  }

  @Override
  public Node visitClearCacheStatement(TableSqlParser.ClearCacheStatementContext ctx) {
    return super.visitClearCacheStatement(ctx);
  }

  @Override
  public Node visitRepairDataStatement(TableSqlParser.RepairDataStatementContext ctx) {
    return super.visitRepairDataStatement(ctx);
  }

  @Override
  public Node visitSetSystemStatusStatement(TableSqlParser.SetSystemStatusStatementContext ctx) {
    return super.visitSetSystemStatusStatement(ctx);
  }

  @Override
  public Node visitShowVersionStatement(TableSqlParser.ShowVersionStatementContext ctx) {
    return super.visitShowVersionStatement(ctx);
  }

  @Override
  public Node visitShowQueriesStatement(TableSqlParser.ShowQueriesStatementContext ctx) {
    return super.visitShowQueriesStatement(ctx);
  }

  @Override
  public Node visitKillQueryStatement(TableSqlParser.KillQueryStatementContext ctx) {
    return super.visitKillQueryStatement(ctx);
  }

  @Override
  public Node visitLoadConfigurationStatement(
      TableSqlParser.LoadConfigurationStatementContext ctx) {
    return super.visitLoadConfigurationStatement(ctx);
  }

  @Override
  public Node visitLocalOrClusterMode(TableSqlParser.LocalOrClusterModeContext ctx) {
    return super.visitLocalOrClusterMode(ctx);
  }

  @Override
  public Node visitStatementDefault(TableSqlParser.StatementDefaultContext ctx) {
    return super.visitStatementDefault(ctx);
  }

  @Override
  public Node visitExplain(TableSqlParser.ExplainContext ctx) {
    return new Explain(getLocation(ctx), (Statement) visit(ctx.query()));
  }

  @Override
  public Node visitExplainAnalyze(TableSqlParser.ExplainAnalyzeContext ctx) {
    return super.visitExplainAnalyze(ctx);
  }

  // ********************** query expressions ********************
  @Override
  public Node visitQuery(TableSqlParser.QueryContext ctx) {
    Query body = (Query) visit(ctx.queryNoWith());

    return new Query(
        getLocation(ctx),
        visitIfPresent(ctx.with(), With.class),
        body.getQueryBody(),
        body.getOrderBy(),
        body.getOffset(),
        body.getLimit());
  }

  @Override
  public Node visitWith(TableSqlParser.WithContext ctx) {
    return new With(
        getLocation(ctx), ctx.RECURSIVE() != null, visit(ctx.namedQuery(), WithQuery.class));
  }

  @Override
  public Node visitNamedQuery(TableSqlParser.NamedQueryContext ctx) {
    if (ctx.columnAliases() != null) {
      List<Identifier> columns = visit(ctx.columnAliases().identifier(), Identifier.class);
      return new WithQuery(
          getLocation(ctx), (Identifier) visit(ctx.name), (Query) visit(ctx.query()), columns);
    } else {
      return new WithQuery(
          getLocation(ctx), (Identifier) visit(ctx.name), (Query) visit(ctx.query()));
    }
  }

  @Override
  public Node visitQueryNoWith(TableSqlParser.QueryNoWithContext ctx) {
    QueryBody term = (QueryBody) visit(ctx.queryTerm());

    Optional<OrderBy> orderBy = Optional.empty();
    if (ctx.ORDER() != null) {
      orderBy =
          Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
    }

    Optional<Offset> offset = Optional.empty();
    if (ctx.OFFSET() != null) {
      Expression rowCount;
      if (ctx.offset.INTEGER_VALUE() != null) {
        rowCount = new LongLiteral(getLocation(ctx.offset.INTEGER_VALUE()), ctx.offset.getText());
      } else {
        rowCount = new Parameter(getLocation(ctx.offset.QUESTION_MARK()), parameterPosition);
        parameterPosition++;
      }
      offset = Optional.of(new Offset(getLocation(ctx.OFFSET()), rowCount));
    }

    Optional<Node> limit = Optional.empty();
    if (ctx.LIMIT() != null) {
      if (ctx.limit == null) {
        throw new IllegalStateException("Missing LIMIT value");
      }
      Expression rowCount;
      if (ctx.limit.ALL() != null) {
        rowCount = new AllRows(getLocation(ctx.limit.ALL()));
      } else if (ctx.limit.rowCount().INTEGER_VALUE() != null) {
        rowCount =
            new LongLiteral(getLocation(ctx.limit.rowCount().INTEGER_VALUE()), ctx.limit.getText());
      } else {
        rowCount =
            new Parameter(getLocation(ctx.limit.rowCount().QUESTION_MARK()), parameterPosition);
        parameterPosition++;
      }

      limit = Optional.of(new Limit(getLocation(ctx.LIMIT()), rowCount));
    }

    if (term instanceof QuerySpecification) {
      // When we have a simple query specification
      // followed by order by, offset, limit or fetch,
      // fold the order by, limit, offset or fetch clauses
      // into the query specification (analyzer/planner
      // expects this structure to resolve references with respect
      // to columns defined in the query specification)
      QuerySpecification query = (QuerySpecification) term;

      return new Query(
          getLocation(ctx),
          Optional.empty(),
          new QuerySpecification(
              getLocation(ctx),
              query.getSelect(),
              query.getFrom(),
              query.getWhere(),
              query.getGroupBy(),
              query.getHaving(),
              orderBy,
              offset,
              limit),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());
    }

    return new Query(getLocation(ctx), Optional.empty(), term, orderBy, offset, limit);
  }

  @Override
  public Node visitQuerySpecification(TableSqlParser.QuerySpecificationContext ctx) {
    Optional<Relation> from = Optional.empty();
    List<SelectItem> selectItems = visit(ctx.selectItem(), SelectItem.class);

    List<Relation> relations = visit(ctx.relation(), Relation.class);
    if (!relations.isEmpty()) {
      // synthesize implicit join nodes
      Iterator<Relation> iterator = relations.iterator();
      Relation relation = iterator.next();

      while (iterator.hasNext()) {
        relation = new Join(getLocation(ctx), Join.Type.IMPLICIT, relation, iterator.next());
      }

      from = Optional.of(relation);
    }

    return new QuerySpecification(
        getLocation(ctx),
        new Select(getLocation(ctx.SELECT()), isDistinct(ctx.setQuantifier()), selectItems),
        from,
        visitIfPresent(ctx.where, Expression.class),
        visitIfPresent(ctx.groupBy(), GroupBy.class),
        visitIfPresent(ctx.having, Expression.class),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Override
  public Node visitSelectSingle(TableSqlParser.SelectSingleContext ctx) {
    if (ctx.identifier() != null) {
      return new SingleColumn(
          getLocation(ctx),
          (Expression) visit(ctx.expression()),
          (Identifier) visit(ctx.identifier()));
    } else {
      return new SingleColumn(getLocation(ctx), (Expression) visit(ctx.expression()));
    }
  }

  @Override
  public Node visitSelectAll(TableSqlParser.SelectAllContext ctx) {
    List<Identifier> aliases = ImmutableList.of();
    if (ctx.columnAliases() != null) {
      aliases = visit(ctx.columnAliases().identifier(), Identifier.class);
    }

    if (ctx.primaryExpression() != null) {
      return new AllColumns(getLocation(ctx), (Expression) visit(ctx.primaryExpression()), aliases);
    } else {
      return new AllColumns(getLocation(ctx), aliases);
    }
  }

  @Override
  public Node visitGroupBy(TableSqlParser.GroupByContext ctx) {
    return new GroupBy(
        getLocation(ctx),
        isDistinct(ctx.setQuantifier()),
        visit(ctx.groupingElement(), GroupingElement.class));
  }

  @Override
  public Node visitTimenGrouping(TableSqlParser.TimenGroupingContext ctx) {
    long startTime = 0;
    long endTime = 0;
    boolean leftCRightO = true;
    if (ctx.timeRange() != null) {
      TimeRange timeRange = (TimeRange) visit(ctx.timeRange());
      startTime = timeRange.getStartTime();
      endTime = timeRange.getEndTime();
      leftCRightO = timeRange.isLeftCRightO();
    }
    // Parse time interval
    TimeDuration interval = DateTimeUtils.constructTimeDuration(ctx.windowInterval.getText());
    TimeDuration slidingStep = interval;
    if (ctx.windowStep != null) {
      slidingStep = DateTimeUtils.constructTimeDuration(ctx.windowStep.getText());
    }

    if (interval.monthDuration <= 0 && interval.nonMonthDuration <= 0) {
      throw new SemanticException(
          "The second parameter time interval should be a positive integer.");
    }

    if (slidingStep.monthDuration <= 0 && slidingStep.nonMonthDuration <= 0) {
      throw new SemanticException(
          "The third parameter time slidingStep should be a positive integer.");
    }
    return new GroupByTime(
        getLocation(ctx), startTime, endTime, interval, slidingStep, leftCRightO);
  }

  @Override
  public Node visitLeftClosedRightOpen(TableSqlParser.LeftClosedRightOpenContext ctx) {
    return getTimeRange(ctx.timeValue(0), ctx.timeValue(1), true);
  }

  @Override
  public Node visitLeftOpenRightClosed(TableSqlParser.LeftOpenRightClosedContext ctx) {
    return getTimeRange(ctx.timeValue(0), ctx.timeValue(1), false);
  }

  private TimeRange getTimeRange(
      TableSqlParser.TimeValueContext left,
      TableSqlParser.TimeValueContext right,
      boolean leftCRightO) {
    long currentTime = CommonDateTimeUtils.currentTime();
    long startTime = parseTimeValue(left, currentTime);
    long endTime = parseTimeValue(right, currentTime);
    if (startTime >= endTime) {
      throw new SemanticException("Start time should be smaller than endTime in GroupBy");
    }
    return new TimeRange(startTime, endTime, leftCRightO);
  }

  private long parseTimeValue(TableSqlParser.TimeValueContext ctx, long currentTime) {
    if (ctx.INTEGER_VALUE() != null) {
      try {
        if (ctx.MINUS() != null) {
          return -Long.parseLong(ctx.INTEGER_VALUE().getText());
        }
        return Long.parseLong(ctx.INTEGER_VALUE().getText());
      } catch (NumberFormatException e) {
        throw new SemanticException(
            String.format("Can not parse %s to long value", ctx.INTEGER_VALUE().getText()));
      }
    } else {
      return parseDateExpression(ctx.dateExpression(), currentTime);
    }
  }

  private Long parseDateExpression(TableSqlParser.DateExpressionContext ctx, long currentTime) {
    long time;
    time = parseDateTimeFormat(ctx.getChild(0).getText(), currentTime, zoneId);
    for (int i = 1; i < ctx.getChildCount(); i = i + 2) {
      if ("+".equals(ctx.getChild(i).getText())) {
        time += DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText(), false);
      } else {
        time -= DateTimeUtils.convertDurationStrToLong(time, ctx.getChild(i + 1).getText(), false);
      }
    }
    return time;
  }

  @Override
  public Node visitVariationGrouping(TableSqlParser.VariationGroupingContext ctx) {
    return super.visitVariationGrouping(ctx);
  }

  @Override
  public Node visitConditionGrouping(TableSqlParser.ConditionGroupingContext ctx) {
    return super.visitConditionGrouping(ctx);
  }

  @Override
  public Node visitSessionGrouping(TableSqlParser.SessionGroupingContext ctx) {
    return super.visitSessionGrouping(ctx);
  }

  @Override
  public Node visitCountGrouping(TableSqlParser.CountGroupingContext ctx) {
    return super.visitCountGrouping(ctx);
  }

  @Override
  public Node visitKeepExpression(TableSqlParser.KeepExpressionContext ctx) {
    return super.visitKeepExpression(ctx);
  }

  @Override
  public Node visitSingleGroupingSet(TableSqlParser.SingleGroupingSetContext ctx) {
    return new SimpleGroupBy(
        getLocation(ctx), visit(ctx.groupingSet().expression(), Expression.class));
  }

  @Override
  public Node visitRollup(TableSqlParser.RollupContext ctx) {
    return new GroupingSets(
        getLocation(ctx),
        ROLLUP,
        ctx.groupingSet().stream()
            .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
            .collect(toList()));
  }

  @Override
  public Node visitCube(TableSqlParser.CubeContext ctx) {
    return new GroupingSets(
        getLocation(ctx),
        CUBE,
        ctx.groupingSet().stream()
            .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
            .collect(toList()));
  }

  @Override
  public Node visitMultipleGroupingSets(TableSqlParser.MultipleGroupingSetsContext ctx) {
    return new GroupingSets(
        getLocation(ctx),
        EXPLICIT,
        ctx.groupingSet().stream()
            .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
            .collect(toList()));
  }

  @Override
  public Node visitSetOperation(TableSqlParser.SetOperationContext ctx) {
    QueryBody left = (QueryBody) visit(ctx.left);
    QueryBody right = (QueryBody) visit(ctx.right);

    boolean distinct = ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null;

    switch (ctx.operator.getType()) {
      case TableSqlLexer.UNION:
        return new Union(getLocation(ctx.UNION()), ImmutableList.of(left, right), distinct);
      case TableSqlLexer.INTERSECT:
        return new Intersect(getLocation(ctx.INTERSECT()), ImmutableList.of(left, right), distinct);
      case TableSqlLexer.EXCEPT:
        return new Except(getLocation(ctx.EXCEPT()), left, right, distinct);
      default:
        throw new IllegalArgumentException("Unsupported set operation: " + ctx.operator.getText());
    }
  }

  @Override
  public Node visitProperty(TableSqlParser.PropertyContext ctx) {
    NodeLocation location = getLocation(ctx);
    Identifier name = (Identifier) visit(ctx.identifier());
    TableSqlParser.PropertyValueContext valueContext = ctx.propertyValue();
    if (valueContext instanceof TableSqlParser.DefaultPropertyValueContext) {
      return new Property(location, name);
    }
    Expression value =
        (Expression)
            visit(((TableSqlParser.NonDefaultPropertyValueContext) valueContext).expression());
    return new Property(location, name, value);
  }

  @Override
  public Node visitTable(TableSqlParser.TableContext ctx) {
    return new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName()));
  }

  @Override
  public Node visitInlineTable(TableSqlParser.InlineTableContext ctx) {
    return new Values(getLocation(ctx), visit(ctx.expression(), Expression.class));
  }

  @Override
  public Node visitSubquery(TableSqlParser.SubqueryContext ctx) {
    return new TableSubquery(getLocation(ctx), (Query) visit(ctx.queryNoWith()));
  }

  @Override
  public Node visitSortItem(TableSqlParser.SortItemContext ctx) {
    return new SortItem(
        getLocation(ctx),
        (Expression) visit(ctx.expression()),
        Optional.ofNullable(ctx.ordering)
            .map(AstBuilder::getOrderingType)
            .orElse(SortItem.Ordering.ASCENDING),
        Optional.ofNullable(ctx.nullOrdering)
            .map(AstBuilder::getNullOrderingType)
            .orElse(SortItem.NullOrdering.UNDEFINED));
  }

  @Override
  public Node visitUnquotedIdentifier(TableSqlParser.UnquotedIdentifierContext ctx) {
    return new Identifier(getLocation(ctx), ctx.getText(), false);
  }

  @Override
  public Node visitQuotedIdentifier(TableSqlParser.QuotedIdentifierContext ctx) {
    String token = ctx.getText();
    String identifier = token.substring(1, token.length() - 1).replace("\"\"", "\"");

    return new Identifier(getLocation(ctx), identifier, true);
  }

  // ***************** boolean expressions ******************
  @Override
  public Node visitLogicalNot(TableSqlParser.LogicalNotContext ctx) {
    return new NotExpression(getLocation(ctx), (Expression) visit(ctx.booleanExpression()));
  }

  @Override
  public Node visitOr(TableSqlParser.OrContext ctx) {
    List<ParserRuleContext> terms =
        flatten(
            ctx,
            element -> {
              if (element instanceof TableSqlParser.OrContext) {
                TableSqlParser.OrContext or = (TableSqlParser.OrContext) element;
                return Optional.of(or.booleanExpression());
              }

              return Optional.empty();
            });

    return new LogicalExpression(
        getLocation(ctx), LogicalExpression.Operator.OR, visit(terms, Expression.class));
  }

  @Override
  public Node visitAnd(TableSqlParser.AndContext ctx) {
    List<ParserRuleContext> terms =
        flatten(
            ctx,
            element -> {
              if (element instanceof TableSqlParser.AndContext) {
                TableSqlParser.AndContext and = (TableSqlParser.AndContext) element;
                return Optional.of(and.booleanExpression());
              }

              return Optional.empty();
            });

    return new LogicalExpression(
        getLocation(ctx), LogicalExpression.Operator.AND, visit(terms, Expression.class));
  }

  private static List<ParserRuleContext> flatten(
      ParserRuleContext root,
      Function<ParserRuleContext, Optional<List<? extends ParserRuleContext>>> extractChildren) {
    List<ParserRuleContext> result = new ArrayList<>();
    Deque<ParserRuleContext> pending = new ArrayDeque<>();
    pending.push(root);

    while (!pending.isEmpty()) {
      ParserRuleContext next = pending.pop();

      Optional<List<? extends ParserRuleContext>> children = extractChildren.apply(next);
      if (!children.isPresent()) {
        result.add(next);
      } else {
        for (int i = children.get().size() - 1; i >= 0; i--) {
          pending.push(children.get().get(i));
        }
      }
    }

    return result;
  }

  // *************** from clause *****************
  @Override
  public Node visitJoinRelation(TableSqlParser.JoinRelationContext ctx) {
    Relation left = (Relation) visit(ctx.left);
    Relation right;

    if (ctx.CROSS() != null) {
      right = (Relation) visit(ctx.right);
      return new Join(getLocation(ctx), Join.Type.CROSS, left, right);
    }

    JoinCriteria criteria;
    if (ctx.NATURAL() != null) {
      right = (Relation) visit(ctx.right);
      criteria = new NaturalJoin();
    } else {
      right = (Relation) visit(ctx.rightRelation);
      if (ctx.joinCriteria().ON() != null) {
        criteria = new JoinOn((Expression) visit(ctx.joinCriteria().booleanExpression()));
      } else if (ctx.joinCriteria().USING() != null) {
        criteria = new JoinUsing(visit(ctx.joinCriteria().identifier(), Identifier.class));
      } else {
        throw new IllegalArgumentException("Unsupported join criteria");
      }
    }

    Join.Type joinType;
    if (ctx.joinType().LEFT() != null) {
      joinType = Join.Type.LEFT;
    } else if (ctx.joinType().RIGHT() != null) {
      joinType = Join.Type.RIGHT;
    } else if (ctx.joinType().FULL() != null) {
      joinType = Join.Type.FULL;
    } else {
      joinType = Join.Type.INNER;
    }

    return new Join(getLocation(ctx), joinType, left, right, criteria);
  }

  @Override
  public Node visitAliasedRelation(TableSqlParser.AliasedRelationContext ctx) {
    Relation child = (Relation) visit(ctx.relationPrimary());

    if (ctx.identifier() == null) {
      return child;
    }

    List<Identifier> aliases = null;
    if (ctx.columnAliases() != null) {
      aliases = visit(ctx.columnAliases().identifier(), Identifier.class);
    }

    return new AliasedRelation(
        getLocation(ctx), child, (Identifier) visit(ctx.identifier()), aliases);
  }

  @Override
  public Node visitTableName(TableSqlParser.TableNameContext ctx) {
    return new Table(getLocation(ctx), getQualifiedName(ctx.qualifiedName()));
  }

  @Override
  public Node visitSubqueryRelation(TableSqlParser.SubqueryRelationContext ctx) {
    return new TableSubquery(getLocation(ctx), (Query) visit(ctx.query()));
  }

  @Override
  public Node visitParenthesizedRelation(TableSqlParser.ParenthesizedRelationContext ctx) {
    return visit(ctx.relation());
  }

  // ********************* predicates *******************

  @Override
  public Node visitPredicated(TableSqlParser.PredicatedContext ctx) {
    if (ctx.predicate() != null) {
      return visit(ctx.predicate());
    }

    return visit(ctx.valueExpression);
  }

  @Override
  public Node visitComparison(TableSqlParser.ComparisonContext ctx) {
    return new ComparisonExpression(
        getLocation(ctx.comparisonOperator()),
        getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
        (Expression) visit(ctx.value),
        (Expression) visit(ctx.right));
  }

  @Override
  public Node visitQuantifiedComparison(TableSqlParser.QuantifiedComparisonContext ctx) {
    return new QuantifiedComparisonExpression(
        getLocation(ctx.comparisonOperator()),
        getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
        getComparisonQuantifier(
            ((TerminalNode) ctx.comparisonQuantifier().getChild(0)).getSymbol()),
        (Expression) visit(ctx.value),
        new SubqueryExpression(getLocation(ctx.query()), (Query) visit(ctx.query())));
  }

  @Override
  public Node visitBetween(TableSqlParser.BetweenContext ctx) {
    Expression expression =
        new BetweenPredicate(
            getLocation(ctx),
            (Expression) visit(ctx.value),
            (Expression) visit(ctx.lower),
            (Expression) visit(ctx.upper));

    if (ctx.NOT() != null) {
      expression = new NotExpression(getLocation(ctx), expression);
    }

    return expression;
  }

  @Override
  public Node visitInList(TableSqlParser.InListContext ctx) {
    Expression result =
        new InPredicate(
            getLocation(ctx),
            (Expression) visit(ctx.value),
            new InListExpression(getLocation(ctx), visit(ctx.expression(), Expression.class)));

    if (ctx.NOT() != null) {
      result = new NotExpression(getLocation(ctx), result);
    }

    return result;
  }

  @Override
  public Node visitInSubquery(TableSqlParser.InSubqueryContext ctx) {
    Expression result =
        new InPredicate(
            getLocation(ctx),
            (Expression) visit(ctx.value),
            new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));

    if (ctx.NOT() != null) {
      result = new NotExpression(getLocation(ctx), result);
    }

    return result;
  }

  @Override
  public Node visitLike(TableSqlParser.LikeContext ctx) {
    Expression result;
    if (ctx.escape != null) {
      result =
          new LikePredicate(
              getLocation(ctx),
              (Expression) visit(ctx.value),
              (Expression) visit(ctx.pattern),
              (Expression) visit(ctx.escape));
    } else {
      result =
          new LikePredicate(
              getLocation(ctx), (Expression) visit(ctx.value), (Expression) visit(ctx.pattern));
    }

    if (ctx.NOT() != null) {
      result = new NotExpression(getLocation(ctx), result);
    }

    return result;
  }

  @Override
  public Node visitNullPredicate(TableSqlParser.NullPredicateContext ctx) {
    Expression child = (Expression) visit(ctx.value);

    if (ctx.NOT() == null) {
      return new IsNullPredicate(getLocation(ctx), child);
    }

    return new IsNotNullPredicate(getLocation(ctx), child);
  }

  @Override
  public Node visitDistinctFrom(TableSqlParser.DistinctFromContext ctx) {
    Expression expression =
        new ComparisonExpression(
            getLocation(ctx),
            ComparisonExpression.Operator.IS_DISTINCT_FROM,
            (Expression) visit(ctx.value),
            (Expression) visit(ctx.right));

    if (ctx.NOT() != null) {
      expression = new NotExpression(getLocation(ctx), expression);
    }

    return expression;
  }

  @Override
  public Node visitExists(TableSqlParser.ExistsContext ctx) {
    return new ExistsPredicate(
        getLocation(ctx), new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));
  }

  // ************** value expressions **************
  @Override
  public Node visitArithmeticUnary(TableSqlParser.ArithmeticUnaryContext ctx) {
    Expression child = (Expression) visit(ctx.valueExpression());

    switch (ctx.operator.getType()) {
      case TableSqlLexer.MINUS:
        return ArithmeticUnaryExpression.negative(getLocation(ctx), child);
      case TableSqlLexer.PLUS:
        return ArithmeticUnaryExpression.positive(getLocation(ctx), child);
      default:
        throw new UnsupportedOperationException("Unsupported sign: " + ctx.operator.getText());
    }
  }

  @Override
  public Node visitArithmeticBinary(TableSqlParser.ArithmeticBinaryContext ctx) {
    return new ArithmeticBinaryExpression(
        getLocation(ctx.operator),
        getArithmeticBinaryOperator(ctx.operator),
        (Expression) visit(ctx.left),
        (Expression) visit(ctx.right));
  }

  @Override
  public Node visitConcatenation(TableSqlParser.ConcatenationContext ctx) {
    return new FunctionCall(
        getLocation(ctx.CONCAT()),
        QualifiedName.of("concat"),
        ImmutableList.of((Expression) visit(ctx.left), (Expression) visit(ctx.right)));
  }

  // ********************* primary expressions **********************
  @Override
  public Node visitParenthesizedExpression(TableSqlParser.ParenthesizedExpressionContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public Node visitRowConstructor(TableSqlParser.RowConstructorContext context) {
    return new Row(getLocation(context), visit(context.expression(), Expression.class));
  }

  @Override
  public Node visitCast(TableSqlParser.CastContext ctx) {
    return new Cast(
        getLocation(ctx), (Expression) visit(ctx.expression()), (DataType) visit(ctx.type()));
  }

  @Override
  public Node visitSpecialDateTimeFunction(TableSqlParser.SpecialDateTimeFunctionContext ctx) {
    CurrentTime.Function function = getDateTimeFunctionType(ctx.name);
    return new CurrentTime(getLocation(ctx), function);
  }

  @Override
  public Node visitTrim(TableSqlParser.TrimContext ctx) {
    if (ctx.FROM() != null && ctx.trimsSpecification() == null && ctx.trimChar == null) {
      throw parseError(
          "The 'trim' function must have specification, char or both arguments when it takes FROM",
          ctx);
    }

    Trim.Specification specification =
        ctx.trimsSpecification() == null
            ? Trim.Specification.BOTH
            : toTrimSpecification((Token) ctx.trimsSpecification().getChild(0).getPayload());
    if (ctx.trimChar != null) {
      return new Trim(
          getLocation(ctx),
          specification,
          (Expression) visit(ctx.trimSource),
          (Expression) visit(ctx.trimChar));
    } else {
      return new Trim(getLocation(ctx), specification, (Expression) visit(ctx.trimSource));
    }
  }

  private static Trim.Specification toTrimSpecification(Token token) {
    switch (token.getType()) {
      case TableSqlLexer.BOTH:
        return Trim.Specification.BOTH;
      case TableSqlLexer.LEADING:
        return Trim.Specification.LEADING;
      case TableSqlLexer.TRAILING:
        return Trim.Specification.TRAILING;
      default:
        throw new IllegalArgumentException("Unsupported trim specification: " + token.getText());
    }
  }

  @Override
  public Node visitSubstring(TableSqlParser.SubstringContext ctx) {
    return new FunctionCall(
        getLocation(ctx),
        QualifiedName.of("substr"),
        visit(ctx.valueExpression(), Expression.class));
  }

  @Override
  public Node visitCurrentDatabase(TableSqlParser.CurrentDatabaseContext ctx) {
    return new CurrentDatabase(getLocation(ctx));
  }

  @Override
  public Node visitCurrentUser(TableSqlParser.CurrentUserContext ctx) {
    return new CurrentUser(getLocation(ctx));
  }

  @Override
  public Node visitSubqueryExpression(TableSqlParser.SubqueryExpressionContext ctx) {
    return new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query()));
  }

  @Override
  public Node visitDereference(TableSqlParser.DereferenceContext ctx) {
    return new DereferenceExpression(
        getLocation(ctx), (Expression) visit(ctx.base), (Identifier) visit(ctx.fieldName));
  }

  @Override
  public Node visitColumnReference(TableSqlParser.ColumnReferenceContext ctx) {
    return visit(ctx.identifier());
  }

  @Override
  public Node visitSimpleCase(TableSqlParser.SimpleCaseContext ctx) {
    if (ctx.elseExpression != null) {
      return new SimpleCaseExpression(
          getLocation(ctx),
          (Expression) visit(ctx.operand),
          visit(ctx.whenClause(), WhenClause.class),
          (Expression) visit(ctx.elseExpression));
    } else {
      return new SimpleCaseExpression(
          getLocation(ctx),
          (Expression) visit(ctx.operand),
          visit(ctx.whenClause(), WhenClause.class));
    }
  }

  @Override
  public Node visitSearchedCase(TableSqlParser.SearchedCaseContext ctx) {
    if (ctx.elseExpression != null) {
      return new SearchedCaseExpression(
          getLocation(ctx),
          visit(ctx.whenClause(), WhenClause.class),
          (Expression) visit(ctx.elseExpression));
    } else {
      return new SearchedCaseExpression(
          getLocation(ctx), visit(ctx.whenClause(), WhenClause.class));
    }
  }

  @Override
  public Node visitWhenClause(TableSqlParser.WhenClauseContext ctx) {
    return new WhenClause(
        getLocation(ctx), (Expression) visit(ctx.condition), (Expression) visit(ctx.result));
  }

  @Override
  public Node visitFunctionCall(TableSqlParser.FunctionCallContext ctx) {

    QualifiedName name = getQualifiedName(ctx.qualifiedName());

    boolean distinct = isDistinct(ctx.setQuantifier());

    if (name.toString().equalsIgnoreCase("if")) {
      check(
          ctx.expression().size() == 2 || ctx.expression().size() == 3,
          "Invalid number of arguments for 'if' function",
          ctx);
      check(!distinct, "DISTINCT not valid for 'if' function", ctx);

      Expression elseExpression = null;
      if (ctx.expression().size() == 3) {
        elseExpression = (Expression) visit(ctx.expression(2));
      }

      return new IfExpression(
          getLocation(ctx),
          (Expression) visit(ctx.expression(0)),
          (Expression) visit(ctx.expression(1)),
          elseExpression);
    }

    if (name.toString().equalsIgnoreCase("nullif")) {
      check(ctx.expression().size() == 2, "Invalid number of arguments for 'nullif' function", ctx);
      check(!distinct, "DISTINCT not valid for 'nullif' function", ctx);

      return new NullIfExpression(
          getLocation(ctx),
          (Expression) visit(ctx.expression(0)),
          (Expression) visit(ctx.expression(1)));
    }

    if (name.toString().equalsIgnoreCase("coalesce")) {
      check(
          ctx.expression().size() >= 2,
          "The 'coalesce' function must have at least two arguments",
          ctx);
      check(!distinct, "DISTINCT not valid for 'coalesce' function", ctx);

      return new CoalesceExpression(getLocation(ctx), visit(ctx.expression(), Expression.class));
    }

    List<Expression> arguments = visit(ctx.expression(), Expression.class);
    if (ctx.label != null) {
      arguments =
          ImmutableList.of(
              new DereferenceExpression(getLocation(ctx.label), (Identifier) visit(ctx.label)));
    }

    return new FunctionCall(getLocation(ctx), name, distinct, arguments);
  }

  // ************** literals **************

  @Override
  public Node visitNullLiteral(TableSqlParser.NullLiteralContext ctx) {
    return new NullLiteral(getLocation(ctx));
  }

  @Override
  public Node visitBasicStringLiteral(TableSqlParser.BasicStringLiteralContext ctx) {
    return new StringLiteral(getLocation(ctx), unquote(ctx.STRING().getText()));
  }

  @Override
  public Node visitUnicodeStringLiteral(TableSqlParser.UnicodeStringLiteralContext ctx) {
    return new StringLiteral(getLocation(ctx), decodeUnicodeLiteral(ctx));
  }

  @Override
  public Node visitBinaryLiteral(TableSqlParser.BinaryLiteralContext ctx) {
    String raw = ctx.BINARY_LITERAL().getText();
    return new BinaryLiteral(getLocation(ctx), unquote(raw.substring(1)));
  }

  @Override
  public Node visitDecimalLiteral(TableSqlParser.DecimalLiteralContext ctx) {
    return new DoubleLiteral(getLocation(ctx), ctx.getText());
  }

  @Override
  public Node visitDoubleLiteral(TableSqlParser.DoubleLiteralContext ctx) {
    return new DoubleLiteral(getLocation(ctx), ctx.getText());
  }

  @Override
  public Node visitIntegerLiteral(TableSqlParser.IntegerLiteralContext ctx) {
    return new LongLiteral(getLocation(ctx), ctx.getText());
  }

  @Override
  public Node visitBooleanLiteral(TableSqlParser.BooleanLiteralContext ctx) {
    return new BooleanLiteral(getLocation(ctx), ctx.getText());
  }

  @Override
  public Node visitParameter(TableSqlParser.ParameterContext ctx) {
    Parameter parameter = new Parameter(getLocation(ctx), parameterPosition);
    parameterPosition++;
    return parameter;
  }

  @Override
  public Node visitIdentifierOrString(TableSqlParser.IdentifierOrStringContext ctx) {
    String s = null;
    if (ctx.identifier() != null) {
      return visit(ctx.identifier());
    } else if (ctx.string() != null) {
      s = ((StringLiteral) visit(ctx.string())).getValue();
    }

    return new Identifier(getLocation(ctx), s);
  }

  @Override
  public Node visitIntervalField(TableSqlParser.IntervalFieldContext ctx) {
    return super.visitIntervalField(ctx);
  }

  // ***************** arguments *****************
  @Override
  public Node visitGenericType(TableSqlParser.GenericTypeContext ctx) {
    List<DataTypeParameter> parameters =
        ctx.typeParameter().stream()
            .map(this::visit)
            .map(DataTypeParameter.class::cast)
            .collect(toImmutableList());

    return new GenericDataType(getLocation(ctx), (Identifier) visit(ctx.identifier()), parameters);
  }

  @Override
  public Node visitTypeParameter(TableSqlParser.TypeParameterContext ctx) {
    if (ctx.INTEGER_VALUE() != null) {
      return new NumericParameter(getLocation(ctx), ctx.getText());
    }

    return new TypeParameter((DataType) visit(ctx.type()));
  }

  // ***************** helpers *****************

  private enum UnicodeDecodeState {
    EMPTY,
    ESCAPED,
    UNICODE_SEQUENCE
  }

  private static String decodeUnicodeLiteral(TableSqlParser.UnicodeStringLiteralContext context) {
    char escape;
    if (context.UESCAPE() != null) {
      String escapeString = unquote(context.STRING().getText());
      check(!escapeString.isEmpty(), "Empty Unicode escape character", context);
      check(
          escapeString.length() == 1, "Invalid Unicode escape character: " + escapeString, context);
      escape = escapeString.charAt(0);
      check(
          isValidUnicodeEscape(escape),
          "Invalid Unicode escape character: " + escapeString,
          context);
    } else {
      escape = '\\';
    }

    String rawContent = unquote(context.UNICODE_STRING().getText().substring(2));
    StringBuilder unicodeStringBuilder = new StringBuilder();
    StringBuilder escapedCharacterBuilder = new StringBuilder();
    int charactersNeeded = 0;
    UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
    for (int i = 0; i < rawContent.length(); i++) {
      char ch = rawContent.charAt(i);
      switch (state) {
        case EMPTY:
          if (ch == escape) {
            state = UnicodeDecodeState.ESCAPED;
          } else {
            unicodeStringBuilder.append(ch);
          }
          break;
        case ESCAPED:
          if (ch == escape) {
            unicodeStringBuilder.append(escape);
            state = UnicodeDecodeState.EMPTY;
          } else if (ch == '+') {
            state = UnicodeDecodeState.UNICODE_SEQUENCE;
            charactersNeeded = 6;
          } else if (isHexDigit(ch)) {
            state = UnicodeDecodeState.UNICODE_SEQUENCE;
            charactersNeeded = 4;
            escapedCharacterBuilder.append(ch);
          } else {
            throw parseError("Invalid hexadecimal digit: " + ch, context);
          }
          break;
        case UNICODE_SEQUENCE:
          check(isHexDigit(ch), "Incomplete escape sequence: " + escapedCharacterBuilder, context);
          escapedCharacterBuilder.append(ch);
          if (charactersNeeded == escapedCharacterBuilder.length()) {
            String currentEscapedCode = escapedCharacterBuilder.toString();
            escapedCharacterBuilder.setLength(0);
            int codePoint = Integer.parseInt(currentEscapedCode, 16);
            check(
                Character.isValidCodePoint(codePoint),
                "Invalid escaped character: " + currentEscapedCode,
                context);
            if (Character.isSupplementaryCodePoint(codePoint)) {
              unicodeStringBuilder.appendCodePoint(codePoint);
            } else {
              char currentCodePoint = (char) codePoint;
              if (Character.isSurrogate(currentCodePoint)) {
                throw parseError(
                    String.format(
                        "Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.",
                        currentEscapedCode),
                    context);
              }
              unicodeStringBuilder.append(currentCodePoint);
            }
            state = UnicodeDecodeState.EMPTY;
            charactersNeeded = -1;
          } else {
            check(
                charactersNeeded > escapedCharacterBuilder.length(),
                "Unexpected escape sequence length: " + escapedCharacterBuilder.length(),
                context);
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    check(
        state == UnicodeDecodeState.EMPTY,
        "Incomplete escape sequence: " + escapedCharacterBuilder.toString(),
        context);
    return unicodeStringBuilder.toString();
  }

  private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
    return Optional.ofNullable(context).map(this::visit).map(clazz::cast);
  }

  private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
    return contexts.stream().map(this::visit).map(clazz::cast).collect(toList());
  }

  private static String unquote(String value) {
    return value.substring(1, value.length() - 1).replace("''", "'");
  }

  private QualifiedName getQualifiedName(TableSqlParser.QualifiedNameContext context) {
    return QualifiedName.of(visit(context.identifier(), Identifier.class));
  }

  private static boolean isDistinct(TableSqlParser.SetQuantifierContext setQuantifier) {
    return setQuantifier != null && setQuantifier.DISTINCT() != null;
  }

  private static boolean isHexDigit(char c) {
    return ((c >= '0') && (c <= '9')) || ((c >= 'A') && (c <= 'F')) || ((c >= 'a') && (c <= 'f'));
  }

  private static boolean isValidUnicodeEscape(char c) {
    return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
  }

  private static Optional<String> getTextIfPresent(ParserRuleContext context) {
    return Optional.ofNullable(context).map(ParseTree::getText);
  }

  private Optional<Identifier> getIdentifierIfPresent(ParserRuleContext context) {
    return Optional.ofNullable(context).map(c -> (Identifier) visit(c));
  }

  private static TsTableColumnCategory getColumnCategory(Token category) {
    if (category == null) {
      return MEASUREMENT;
    }
    switch (category.getType()) {
      case TableSqlLexer.ID:
        return ID;
      case TableSqlLexer.ATTRIBUTE:
        return ATTRIBUTE;
      case TableSqlLexer.TIME:
        return TIME;
      case TableSqlLexer.MEASUREMENT:
        return MEASUREMENT;
      default:
        throw new UnsupportedOperationException(
            "Unsupported ColumnCategory: " + category.getText());
    }
  }

  private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator) {
    switch (operator.getType()) {
      case TableSqlLexer.PLUS:
        return ArithmeticBinaryExpression.Operator.ADD;
      case TableSqlLexer.MINUS:
        return ArithmeticBinaryExpression.Operator.SUBTRACT;
      case TableSqlLexer.ASTERISK:
        return ArithmeticBinaryExpression.Operator.MULTIPLY;
      case TableSqlLexer.SLASH:
        return ArithmeticBinaryExpression.Operator.DIVIDE;
      case TableSqlLexer.PERCENT:
        return ArithmeticBinaryExpression.Operator.MODULUS;
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }
  }

  private static ComparisonExpression.Operator getComparisonOperator(Token symbol) {
    switch (symbol.getType()) {
      case TableSqlLexer.EQ:
        return ComparisonExpression.Operator.EQUAL;
      case TableSqlLexer.NEQ:
        return ComparisonExpression.Operator.NOT_EQUAL;
      case TableSqlLexer.LT:
        return ComparisonExpression.Operator.LESS_THAN;
      case TableSqlLexer.LTE:
        return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
      case TableSqlLexer.GT:
        return ComparisonExpression.Operator.GREATER_THAN;
      case TableSqlLexer.GTE:
        return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
      default:
        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }
  }

  private static CurrentTime.Function getDateTimeFunctionType(Token token) {
    switch (token.getType()) {
      case TableSqlLexer.CURRENT_DATE:
        return CurrentTime.Function.DATE;
      case TableSqlLexer.CURRENT_TIME:
        return CurrentTime.Function.TIME;
      case TableSqlLexer.CURRENT_TIMESTAMP:
      case TableSqlLexer.NOW:
        return CurrentTime.Function.TIMESTAMP;
      case TableSqlLexer.LOCALTIME:
        return CurrentTime.Function.LOCALTIME;
      case TableSqlLexer.LOCALTIMESTAMP:
        return CurrentTime.Function.LOCALTIMESTAMP;
      default:
        throw new IllegalArgumentException("Unsupported special function: " + token.getText());
    }
  }

  private static SortItem.NullOrdering getNullOrderingType(Token token) {
    switch (token.getType()) {
      case TableSqlLexer.FIRST:
        return SortItem.NullOrdering.FIRST;
      case TableSqlLexer.LAST:
        return SortItem.NullOrdering.LAST;
      default:
        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }
  }

  private static SortItem.Ordering getOrderingType(Token token) {
    switch (token.getType()) {
      case TableSqlLexer.ASC:
        return SortItem.Ordering.ASCENDING;
      case TableSqlLexer.DESC:
        return SortItem.Ordering.DESCENDING;
      default:
        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }
  }

  private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol) {
    switch (symbol.getType()) {
      case TableSqlLexer.ALL:
        return QuantifiedComparisonExpression.Quantifier.ALL;
      case TableSqlLexer.ANY:
        return QuantifiedComparisonExpression.Quantifier.ANY;
      case TableSqlLexer.SOME:
        return QuantifiedComparisonExpression.Quantifier.SOME;
      default:
        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }
  }

  private List<Identifier> getIdentifiers(List<TableSqlParser.IdentifierContext> identifiers) {
    return identifiers.stream().map(context -> (Identifier) visit(context)).collect(toList());
  }

  private static void check(boolean condition, String message, ParserRuleContext context) {
    if (!condition) {
      throw parseError(message, context);
    }
  }

  private NodeLocation getLocation(TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  private NodeLocation getLocation(ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  private NodeLocation getLocation(Token token) {
    requireNonNull(token, "token is null");
    return baseLocation != null
        ? new NodeLocation(
            token.getLine() + baseLocation.getLineNumber() - 1,
            token.getCharPositionInLine()
                + 1
                + (token.getLine() == 1 ? baseLocation.getColumnNumber() : 0))
        : new NodeLocation(token.getLine(), token.getCharPositionInLine() + 1);
  }

  private static ParsingException parseError(String message, ParserRuleContext context) {
    return new ParsingException(
        message,
        null,
        context.getStart().getLine(),
        context.getStart().getCharPositionInLine() + 1);
  }

  private static void validateArgumentAlias(Identifier alias, ParserRuleContext context) {
    check(
        alias.isDelimited() || !alias.getValue().equalsIgnoreCase("COPARTITION"),
        "The word \"COPARTITION\" is ambiguous in this context. "
            + "To alias an argument, precede the alias with \"AS\". "
            + "To specify co-partitioning, change the argument order so that the last argument cannot be aliased.",
        context);
  }
}
