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

package org.apache.iotdb.db.queryengine.plan.tree.sql.ast;

import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.DeleteDataStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.InsertRowStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.InsertRowsStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.InsertStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.InsertTabletStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.LoadTsFileStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.internal.DeviceSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.internal.InternalBatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.internal.InternalCreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.internal.InternalCreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.internal.SeriesSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CountDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CountLevelTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CountNodesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CountTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CountTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CreateMultiTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.DropContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.DropFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.DropTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.MigrateRegionStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowChildNodesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowChildPathsStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowClusterIdStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowContinuousQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowCurrentTimestampStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowDevicesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowTriggersStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.ShowVariablesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.AlterPipeStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.CreatePipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.DropPipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.ShowPipePluginsStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.pipe.StopPipeStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.subscription.CreateTopicStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.subscription.DropTopicStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.subscription.ShowSubscriptionsStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.subscription.ShowTopicsStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.AlterSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.BatchActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.ShowPathsUsingTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.view.AlterLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.view.DeleteLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.view.RenameLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.metadata.view.ShowLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.pipe.PipeEnrichedStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.AuthorStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.ClearCacheStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.ExplainAnalyzeStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.ExplainStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.MergeStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.SetConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.SetSystemStatusStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.ShowQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.ShowVersionStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.StartRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.StopRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.TestConnectionStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.quota.SetSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.quota.SetThrottleQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.tree.sql.ast.sys.quota.ShowThrottleQuotaStatement;

/**
 * This class provides a visitor of {@link StatementNode}, which can be extended to create a visitor
 * which only needs to handle a subset of the available methods.
 *
 * @param <R> The return type of the visit operation.
 * @param <C> The context information during visiting.
 */
public abstract class StatementVisitor<R, C> {

  public R process(StatementNode node, C context) {
    return node.accept(this, context);
  }

  /** Top Level Description */
  public abstract R visitNode(StatementNode node, C context);

  public R visitStatement(Statement statement, C context) {
    return visitNode(statement, context);
  }

  /** Data Definition Language (DDL) */

  // Create Timeseries
  public R visitCreateTimeseries(CreateTimeSeriesStatement createTimeSeriesStatement, C context) {
    return visitStatement(createTimeSeriesStatement, context);
  }

  // Create Aligned Timeseries
  public R visitCreateAlignedTimeseries(
      CreateAlignedTimeSeriesStatement createAlignedTimeSeriesStatement, C context) {
    return visitStatement(createAlignedTimeSeriesStatement, context);
  }

  // Create Timeseries by device
  public R visitInternalCreateTimeseries(
      InternalCreateTimeSeriesStatement internalCreateTimeSeriesStatement, C context) {
    return visitStatement(internalCreateTimeSeriesStatement, context);
  }

  // Create Multi Timeseries
  public R visitCreateMultiTimeseries(
      CreateMultiTimeSeriesStatement createMultiTimeSeriesStatement, C context) {
    return visitStatement(createMultiTimeSeriesStatement, context);
  }

  // Alter Timeseries
  public R visitAlterTimeseries(AlterTimeSeriesStatement alterTimeSeriesStatement, C context) {
    return visitStatement(alterTimeSeriesStatement, context);
  }

  public R visitDeleteTimeseries(DeleteTimeSeriesStatement deleteTimeSeriesStatement, C context) {
    return visitStatement(deleteTimeSeriesStatement, context);
  }

  public R visitDeleteStorageGroup(DeleteDatabaseStatement deleteDatabaseStatement, C context) {
    return visitStatement(deleteDatabaseStatement, context);
  }

  public R visitSetDatabase(DatabaseSchemaStatement databaseSchemaStatement, C context) {
    return visitStatement(databaseSchemaStatement, context);
  }

  public R visitAlterDatabase(DatabaseSchemaStatement databaseSchemaStatement, C context) {
    return visitStatement(databaseSchemaStatement, context);
  }

  // Alter TTL
  public R visitSetTTL(SetTTLStatement setTTLStatement, C context) {
    return visitStatement(setTTLStatement, context);
  }

  public R visitUnSetTTL(UnSetTTLStatement unSetTTLStatement, C context) {
    return visitStatement(unSetTTLStatement, context);
  }

  public R visitShowTTL(ShowTTLStatement showTTLStatement, C context) {
    return visitStatement(showTTLStatement, context);
  }

  public R visitShowVariables(ShowVariablesStatement showVariablesStatement, C context) {
    return visitStatement(showVariablesStatement, context);
  }

  public R visitShowCluster(ShowClusterStatement showClusterStatement, C context) {
    return visitStatement(showClusterStatement, context);
  }

  public R visitShowClusterId(ShowClusterIdStatement showClusterIdStatement, C context) {
    return visitStatement(showClusterIdStatement, context);
  }

  public R visitTestConnection(TestConnectionStatement testConnectionStatement, C context) {
    return visitStatement(testConnectionStatement, context);
  }

  // UDF
  public R visitCreateFunction(CreateFunctionStatement createFunctionStatement, C context) {
    return visitStatement(createFunctionStatement, context);
  }

  public R visitDropFunction(DropFunctionStatement dropFunctionStatement, C context) {
    return visitStatement(dropFunctionStatement, context);
  }

  public R visitShowFunctions(ShowFunctionsStatement showFunctionsStatement, C context) {
    return visitStatement(showFunctionsStatement, context);
  }

  // Trigger
  public R visitCreateTrigger(CreateTriggerStatement createTriggerStatement, C context) {
    return visitStatement(createTriggerStatement, context);
  }

  public R visitDropTrigger(DropTriggerStatement dropTriggerStatement, C context) {
    return visitStatement(dropTriggerStatement, context);
  }

  public R visitShowTriggers(ShowTriggersStatement showTriggersStatement, C context) {
    return visitStatement(showTriggersStatement, context);
  }

  // Pipe Plugin
  public R visitCreatePipePlugin(CreatePipePluginStatement createPipePluginStatement, C context) {
    return visitStatement(createPipePluginStatement, context);
  }

  public R visitDropPipePlugin(DropPipePluginStatement dropPipePluginStatement, C context) {
    return visitStatement(dropPipePluginStatement, context);
  }

  public R visitShowPipePlugins(ShowPipePluginsStatement showPipePluginsStatement, C context) {
    return visitStatement(showPipePluginsStatement, context);
  }

  // Create Logical View
  public R visitCreateLogicalView(
      CreateLogicalViewStatement createLogicalViewStatement, C context) {
    return visitStatement(createLogicalViewStatement, context);
  }

  public R visitDeleteLogicalView(
      DeleteLogicalViewStatement deleteLogicalViewStatement, C context) {
    return visitStatement(deleteLogicalViewStatement, context);
  }

  public R visitShowLogicalView(ShowLogicalViewStatement showLogicalViewStatement, C context) {
    return visitStatement(showLogicalViewStatement, context);
  }

  public R visitRenameLogicalView(
      RenameLogicalViewStatement renameLogicalViewStatement, C context) {
    return visitStatement(renameLogicalViewStatement, context);
  }

  public R visitAlterLogicalView(AlterLogicalViewStatement alterLogicalViewStatement, C context) {
    return visitStatement(alterLogicalViewStatement, context);
  }

  /** Data Manipulation Language (DML) */

  // Select Statement
  public R visitQuery(QueryStatement queryStatement, C context) {
    return visitStatement(queryStatement, context);
  }

  // Insert Statement
  public R visitInsert(InsertStatement insertStatement, C context) {
    return visitStatement(insertStatement, context);
  }

  public R visitInsertTablet(InsertTabletStatement insertTabletStatement, C context) {
    return visitStatement(insertTabletStatement, context);
  }

  public R visitLoadFile(LoadTsFileStatement loadTsFileStatement, C context) {
    return visitStatement(loadTsFileStatement, context);
  }

  public R visitInsertRow(InsertRowStatement insertRowStatement, C context) {
    return visitStatement(insertRowStatement, context);
  }

  public R visitInsertRows(InsertRowsStatement insertRowsStatement, C context) {
    return visitStatement(insertRowsStatement, context);
  }

  public R visitInsertMultiTablets(
      InsertMultiTabletsStatement insertMultiTabletsStatement, C context) {
    return visitStatement(insertMultiTabletsStatement, context);
  }

  public R visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement, C context) {
    return visitStatement(insertRowsOfOneDeviceStatement, context);
  }

  public R visitPipeEnrichedStatement(PipeEnrichedStatement pipeEnrichedStatement, C context) {
    return visitStatement(pipeEnrichedStatement, context);
  }

  /** Data Control Language (DCL) */
  public R visitAuthor(AuthorStatement authorStatement, C context) {
    return visitStatement(authorStatement, context);
  }

  public R visitShowStorageGroup(ShowDatabaseStatement showDatabaseStatement, C context) {
    return visitStatement(showDatabaseStatement, context);
  }

  public R visitShowTimeSeries(ShowTimeSeriesStatement showTimeSeriesStatement, C context) {
    return visitStatement(showTimeSeriesStatement, context);
  }

  public R visitShowDevices(ShowDevicesStatement showDevicesStatement, C context) {
    return visitStatement(showDevicesStatement, context);
  }

  public R visitCountStorageGroup(CountDatabaseStatement countDatabaseStatement, C context) {
    return visitStatement(countDatabaseStatement, context);
  }

  public R visitCountDevices(CountDevicesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitCountTimeSeries(CountTimeSeriesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitCountLevelTimeSeries(CountLevelTimeSeriesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitCountNodes(CountNodesStatement countStatement, C context) {
    return visitStatement(countStatement, context);
  }

  public R visitSeriesSchemaFetch(
      SeriesSchemaFetchStatement seriesSchemaFetchStatement, C context) {
    return visitStatement(seriesSchemaFetchStatement, context);
  }

  public R visitDeviceSchemaFetch(
      DeviceSchemaFetchStatement deviceSchemaFetchStatement, C context) {
    return visitStatement(deviceSchemaFetchStatement, context);
  }

  public R visitShowChildPaths(ShowChildPathsStatement showChildPathsStatement, C context) {
    return visitStatement(showChildPathsStatement, context);
  }

  public R visitShowChildNodes(ShowChildNodesStatement showChildNodesStatement, C context) {
    return visitStatement(showChildNodesStatement, context);
  }

  public R visitExplain(ExplainStatement explainStatement, C context) {
    return visitStatement(explainStatement, context);
  }

  public R visitExplainAnalyze(ExplainAnalyzeStatement explainAnalyzeStatement, C context) {
    return visitStatement(explainAnalyzeStatement, context);
  }

  public R visitDeleteData(DeleteDataStatement deleteDataStatement, C context) {
    return visitStatement(deleteDataStatement, context);
  }

  public R visitMerge(MergeStatement mergeStatement, C context) {
    return visitStatement(mergeStatement, context);
  }

  public R visitFlush(FlushStatement flushStatement, C context) {
    return visitStatement(flushStatement, context);
  }

  public R visitClearCache(ClearCacheStatement clearCacheStatement, C context) {
    return visitStatement(clearCacheStatement, context);
  }

  public R visitSetConfiguration(SetConfigurationStatement setConfigurationStatement, C context) {
    return visitStatement(setConfigurationStatement, context);
  }

  public R visitStartRepairData(StartRepairDataStatement startRepairDataStatement, C context) {
    return visitStatement(startRepairDataStatement, context);
  }

  public R visitStopRepairData(StopRepairDataStatement stopRepairDataStatement, C context) {
    return visitStatement(stopRepairDataStatement, context);
  }

  public R visitLoadConfiguration(
      LoadConfigurationStatement loadConfigurationStatement, C context) {
    return visitStatement(loadConfigurationStatement, context);
  }

  public R visitSetSystemStatus(SetSystemStatusStatement setSystemStatusStatement, C context) {
    return visitStatement(setSystemStatusStatement, context);
  }

  public R visitKillQuery(KillQueryStatement killQueryStatement, C context) {
    return visitStatement(killQueryStatement, context);
  }

  public R visitShowQueries(ShowQueriesStatement showQueriesStatement, C context) {
    return visitStatement(showQueriesStatement, context);
  }

  public R visitShowRegion(ShowRegionStatement showRegionStatement, C context) {
    return visitStatement(showRegionStatement, context);
  }

  public R visitShowDataNodes(ShowDataNodesStatement showDataNodesStatement, C context) {
    return visitStatement(showDataNodesStatement, context);
  }

  public R visitShowConfigNodes(ShowConfigNodesStatement showConfigNodesStatement, C context) {
    return visitStatement(showConfigNodesStatement, context);
  }

  public R visitShowVersion(ShowVersionStatement showVersionStatement, C context) {
    return visitStatement(showVersionStatement, context);
  }

  public R visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createTemplateStatement, C context) {
    return visitStatement(createTemplateStatement, context);
  }

  public R visitShowNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement, C context) {
    return visitStatement(showNodesInSchemaTemplateStatement, context);
  }

  public R visitShowSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement, C context) {
    return visitStatement(showSchemaTemplateStatement, context);
  }

  public R visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, C context) {
    return visitStatement(setSchemaTemplateStatement, context);
  }

  public R visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, C context) {
    return visitStatement(showPathSetTemplateStatement, context);
  }

  public R visitActivateTemplate(ActivateTemplateStatement activateTemplateStatement, C context) {
    return visitStatement(activateTemplateStatement, context);
  }

  public R visitBatchActivateTemplate(
      BatchActivateTemplateStatement batchActivateTemplateStatement, C context) {
    return visitStatement(batchActivateTemplateStatement, context);
  }

  public R visitShowPathsUsingTemplate(
      ShowPathsUsingTemplateStatement showPathsUsingTemplateStatement, C context) {
    return visitStatement(showPathsUsingTemplateStatement, context);
  }

  public R visitAlterSchemaTemplate(
      AlterSchemaTemplateStatement alterSchemaTemplateStatement, C context) {
    return visitStatement(alterSchemaTemplateStatement, context);
  }

  public R visitShowPipes(ShowPipesStatement showPipesStatement, C context) {
    return visitStatement(showPipesStatement, context);
  }

  public R visitCreatePipe(CreatePipeStatement createPipeStatement, C context) {
    return visitStatement(createPipeStatement, context);
  }

  public R visitAlterPipe(AlterPipeStatement alterPipeStatement, C context) {
    return visitStatement(alterPipeStatement, context);
  }

  public R visitDropPipe(DropPipeStatement dropPipeStatement, C context) {
    return visitStatement(dropPipeStatement, context);
  }

  public R visitStartPipe(StartPipeStatement startPipeStatement, C context) {
    return visitStatement(startPipeStatement, context);
  }

  public R visitStopPipe(StopPipeStatement stopPipeStatement, C context) {
    return visitStatement(stopPipeStatement, context);
  }

  public R visitCreateTopic(CreateTopicStatement createTopicStatement, C context) {
    return visitStatement(createTopicStatement, context);
  }

  public R visitDropTopic(DropTopicStatement dropTopicStatement, C context) {
    return visitStatement(dropTopicStatement, context);
  }

  public R visitShowTopics(ShowTopicsStatement showTopicsStatement, C context) {
    return visitStatement(showTopicsStatement, context);
  }

  public R visitShowSubscriptions(
      ShowSubscriptionsStatement showSubscriptionsStatement, C context) {
    return visitStatement(showSubscriptionsStatement, context);
  }

  public R visitGetRegionId(GetRegionIdStatement getRegionIdStatement, C context) {
    return visitStatement(getRegionIdStatement, context);
  }

  public R visitGetSeriesSlotList(
      GetSeriesSlotListStatement getSeriesSlotListStatement, C context) {
    return visitStatement(getSeriesSlotListStatement, context);
  }

  public R visitGetTimeSlotList(GetTimeSlotListStatement getTimeSlotListStatement, C context) {
    return visitStatement(getTimeSlotListStatement, context);
  }

  public R visitCountTimeSlotList(
      CountTimeSlotListStatement countTimeSlotListStatement, C context) {
    return visitStatement(countTimeSlotListStatement, context);
  }

  public R visitMigrateRegion(MigrateRegionStatement migrateRegionStatement, C context) {
    return visitStatement(migrateRegionStatement, context);
  }

  public R visitDeactivateTemplate(
      DeactivateTemplateStatement deactivateTemplateStatement, C context) {
    return visitStatement(deactivateTemplateStatement, context);
  }

  public R visitCreateContinuousQuery(
      CreateContinuousQueryStatement createContinuousQueryStatement, C context) {
    return visitStatement(createContinuousQueryStatement, context);
  }

  public R visitDropContinuousQuery(
      DropContinuousQueryStatement dropContinuousQueryStatement, C context) {
    return visitStatement(dropContinuousQueryStatement, context);
  }

  public R visitShowContinuousQueries(
      ShowContinuousQueriesStatement showContinuousQueriesStatement, C context) {
    return visitStatement(showContinuousQueriesStatement, context);
  }

  public R visitUnsetSchemaTemplate(
      UnsetSchemaTemplateStatement unsetSchemaTemplateStatement, C context) {
    return visitStatement(unsetSchemaTemplateStatement, context);
  }

  public R visitDropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement, C context) {
    return visitStatement(dropSchemaTemplateStatement, context);
  }

  public R visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateStatement internalBatchActivateTemplateStatement, C context) {
    return visitStatement(internalBatchActivateTemplateStatement, context);
  }

  public R visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesStatement internalCreateMultiTimeSeriesStatement, C context) {
    return visitStatement(internalCreateMultiTimeSeriesStatement, context);
  }

  public R visitSetSpaceQuota(SetSpaceQuotaStatement setSpaceQuotaStatement, C context) {
    return visitStatement(setSpaceQuotaStatement, context);
  }

  public R visitShowSpaceQuota(ShowSpaceQuotaStatement showSpaceQuotaStatement, C context) {
    return visitStatement(showSpaceQuotaStatement, context);
  }

  public R visitSetThrottleQuota(SetThrottleQuotaStatement setThrottleQuotaStatement, C context) {
    return visitStatement(setThrottleQuotaStatement, context);
  }

  public R visitShowThrottleQuota(
      ShowThrottleQuotaStatement showThrottleQuotaStatement, C context) {
    return visitStatement(showThrottleQuotaStatement, context);
  }

  public R visitShowCurrentTimestamp(
      ShowCurrentTimestampStatement showCurrentTimestampStatement, C context) {
    return visitStatement(showCurrentTimestampStatement, context);
  }
}
