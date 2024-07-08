/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.table.planner;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.table.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.table.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.table.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.table.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.table.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.table.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.table.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.AliasedRelation;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Except;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Intersect;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Join;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Node;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.QualifiedName;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.QuerySpecification;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.SubqueryExpression;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.TableSubquery;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Union;
import org.apache.iotdb.db.queryengine.plan.table.sql.ast.Values;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TablePlanner extends AstVisitor<TablePlan, Void> {
  private final Analysis analysis;
  private final SymbolAllocator symbolAllocator;
  private final MPPQueryContext queryContext;
  private final QueryId idAllocator;
  private final SessionInfo sessionInfo;
  private final Map<NodeRef<Node>, TablePlan> recursiveSubqueries;

  public TablePlanner(
      Analysis analysis,
      SymbolAllocator symbolAllocator,
      MPPQueryContext queryContext,
      SessionInfo sessionInfo,
      Map<NodeRef<Node>, TablePlan> recursiveSubqueries) {
    requireNonNull(analysis, "analysis is null");
    requireNonNull(symbolAllocator, "symbolAllocator is null");
    requireNonNull(queryContext, "queryContext is null");
    requireNonNull(sessionInfo, "session is null");
    requireNonNull(recursiveSubqueries, "recursiveSubqueries is null");

    this.analysis = analysis;
    this.symbolAllocator = symbolAllocator;
    this.queryContext = queryContext;
    this.idAllocator = queryContext.getQueryId();
    this.sessionInfo = sessionInfo;
    this.recursiveSubqueries = recursiveSubqueries;
  }

  @Override
  protected TablePlan visitQuery(Query node, Void context) {
    return new QueryPlanner(
            analysis, symbolAllocator, queryContext, sessionInfo, recursiveSubqueries)
        .plan(node);
  }

  @Override
  protected TablePlan visitTable(Table table, Void context) {
    // is this a recursive reference in expandable named query? If so, there's base relation already
    // planned.
    TablePlan expansion = recursiveSubqueries.get(NodeRef.of(table));
    if (expansion != null) {
      // put the pre-planned recursive subquery in the actual outer context to enable resolving
      // correlation
      return new TablePlan(expansion.getRoot(), expansion.getScope(), expansion.getFieldMappings());
    }

    Scope scope = analysis.getScope(table);
    ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<Symbol, ColumnSchema> symbolToColumnSchema = ImmutableMap.builder();
    Collection<Field> fields = scope.getRelationType().getAllFields();
    // on the basis of that the order of fields is same with the column category order of segments
    // in DeviceEntry
    Map<Symbol, Integer> idAndAttributeIndexMap = new HashMap<>();
    int idIndex = 0, attributeIndex = 0;
    for (Field field : fields) {
      Symbol symbol = symbolAllocator.newSymbol(field);
      outputSymbolsBuilder.add(symbol);
      TsTableColumnCategory category = field.getColumnCategory();
      symbolToColumnSchema.put(
          symbol,
          new ColumnSchema(
              field.getName().orElse(null), field.getType(), field.isHidden(), category));
      if (category == TsTableColumnCategory.ID) {
        idAndAttributeIndexMap.put(symbol, idIndex++);
      } else if (category == TsTableColumnCategory.ATTRIBUTE) {
        idAndAttributeIndexMap.put(symbol, attributeIndex++);
      }
    }

    List<Symbol> outputSymbols = outputSymbolsBuilder.build();
    QualifiedName qualifiedName = analysis.getRelationName(table);
    if (!qualifiedName.getPrefix().isPresent()) {
      throw new IllegalStateException("Table " + table.getName() + " has no prefix!");
    }

    TableScanNode tableScanNode =
        new TableScanNode(
            idAllocator.genPlanNodeId(),
            new QualifiedObjectName(
                qualifiedName.getPrefix().map(QualifiedName::toString).orElse(null),
                qualifiedName.getSuffix()),
            outputSymbols,
            symbolToColumnSchema.build(),
            idAndAttributeIndexMap);

    return new TablePlan(tableScanNode, scope, outputSymbols);

    // Collection<Field> fields = analysis.getMaterializedViewStorageTableFields(node);
    // Query namedQuery = analysis.getNamedQuery(node);
    // Collection<Field> fields = analysis.getMaterializedViewStorageTableFields(node);
    // plan = addRowFilters(node, plan);
    // plan = addColumnMasks(node, plan);
  }

  @Override
  protected TablePlan visitQuerySpecification(QuerySpecification node, Void context) {
    return new QueryPlanner(
            analysis, symbolAllocator, queryContext, sessionInfo, recursiveSubqueries)
        .plan(node);
  }

  @Override
  protected TablePlan visitNode(Node node, Void context) {
    throw new IllegalStateException("Unsupported node type: " + node.getClass().getName());
  }

  // ================================ Implemented later =====================================
  @Override
  protected TablePlan visitTableSubquery(TableSubquery node, Void context) {
    TablePlan plan = process(node.getQuery(), context);
    // TODO transmit outerContext
    return new TablePlan(plan.getRoot(), analysis.getScope(node), plan.getFieldMappings());
  }

  @Override
  protected TablePlan visitValues(Values node, Void context) {
    throw new IllegalStateException("Values is not supported in current version.");
  }

  @Override
  protected TablePlan visitSubqueryExpression(SubqueryExpression node, Void context) {
    throw new IllegalStateException("SubqueryExpression is not supported in current version.");
  }

  @Override
  protected TablePlan visitJoin(Join node, Void context) {
    throw new IllegalStateException("Join is not supported in current version.");
  }

  @Override
  protected TablePlan visitAliasedRelation(AliasedRelation node, Void context) {
    throw new IllegalStateException("AliasedRelation is not supported in current version.");
  }

  @Override
  protected TablePlan visitIntersect(Intersect node, Void context) {
    throw new IllegalStateException("Intersect is not supported in current version.");
  }

  @Override
  protected TablePlan visitUnion(Union node, Void context) {
    throw new IllegalStateException("Union is not supported in current version.");
  }

  @Override
  protected TablePlan visitExcept(Except node, Void context) {
    throw new IllegalStateException("Except is not supported in current version.");
  }
}
