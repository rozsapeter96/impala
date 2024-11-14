// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.planner;

import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.impala.analysis.Expr;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TSinkAction;
import org.apache.impala.thrift.TSortingOrder;
import org.apache.impala.util.ExprUtil;

import com.google.common.base.Preconditions;

/**
 * A DataSink that writes into a table.
 *
 */
public abstract class TableSink extends DataSink {

  /**
   * Enum to specify the sink operation type.
   */
  public enum Op {
    INSERT {
      @Override
      public String toExplainString() { return "INSERT INTO"; }

      @Override
      public TSinkAction toThrift() { return TSinkAction.INSERT; }
    },
    UPDATE {
      @Override
      public String toExplainString() { return "UPDATE"; }

      @Override
      public TSinkAction toThrift() { return TSinkAction.UPDATE; }
    },
    UPSERT {
      @Override
      public String toExplainString() { return "UPSERT INTO"; }

      @Override
      public TSinkAction toThrift() { return TSinkAction.UPSERT; }
    },
    DELETE {
      @Override
      public String toExplainString() { return "DELETE FROM"; }

      @Override
      public TSinkAction toThrift() { return TSinkAction.DELETE; }
    };

    public abstract String toExplainString();

    public abstract TSinkAction toThrift();
  }

  // Table which is to be populated by this sink.
  protected final FeTable targetTable_;
  // The type of operation to be performed by this sink.
  protected final Op sinkOp_;
  // One expression per result column for the query. Always non-null.
  protected final List<Expr> outputExprs_;

  public TableSink(FeTable targetTable, Op sinkAction, List<Expr> outputExprs) {
    Preconditions.checkState(outputExprs != null);
    targetTable_ = targetTable;
    sinkOp_ = sinkAction;
    outputExprs_ = outputExprs;
  }
  /**
   * Returns an output sink appropriate for writing to the given table.
   * Not all Ops are supported for all tables.
   */
  public static TableSink create(FeTable table, Op sinkAction,
      List<Expr> partitionKeyExprs, List<Expr> outputExprs,
      TableSinkArgs args) {
    Preconditions.checkNotNull(partitionKeyExprs);
    Preconditions.checkNotNull(args.referencedColumns);
    Preconditions.checkNotNull(args.sortProperties.first);
    TableSink sink = null;

    if (table instanceof FeFsTable) {
      if (table instanceof FeIcebergTable) {
        if (sinkAction == Op.INSERT) {
          sink = new HdfsTableSink(table, partitionKeyExprs,
              outputExprs, args.overwrite,
              args.inputIsClustered, args.sortProperties, args.writeId,
              args.maxTableSinks, args.isResultSink);
        } else if (sinkAction == Op.DELETE) {
          sink = new IcebergBufferedDeleteSink((FeIcebergTable) table, partitionKeyExprs,
              outputExprs, args.deleteTableId, args.maxTableSinks);
        } else {
          // Other SINK actions are either not supported or created directly.
          Preconditions.checkState(false);
        }
      } else {
        // Hdfs only supports inserts.
        Preconditions.checkState(sinkAction == Op.INSERT);
        // Referenced columns don't make sense for an Hdfs table.
        Preconditions.checkState(args.referencedColumns.isEmpty());
        sink = new HdfsTableSink(table, partitionKeyExprs,
            outputExprs, args.overwrite,
            args.inputIsClustered, args.sortProperties, args.writeId,
            args.maxTableSinks, args.isResultSink);
      }
    } else if (table instanceof FeHBaseTable) {
      // HBase only supports inserts.
      Preconditions.checkState(sinkAction == Op.INSERT);
      // Partition clause doesn't make sense for an HBase table.
      Preconditions.checkState(partitionKeyExprs.isEmpty());
      // HBase doesn't have a way to perform INSERT OVERWRITE
      Preconditions.checkState(args.overwrite == false);
      // Referenced columns don't make sense for an HBase table.
      Preconditions.checkState(args.referencedColumns.isEmpty());
      // Sort columns are not supported for HBase tables.
      Preconditions.checkState(args.sortProperties.first.isEmpty());
      // Create the HBaseTableSink and return it.
      sink = new HBaseTableSink(table, outputExprs);
    } else if (table instanceof FeKuduTable) {
      // Kudu doesn't have a way to perform INSERT OVERWRITE.
      Preconditions.checkState(args.overwrite == false);
      // Sort columns are not supported for Kudu tables.
      Preconditions.checkState(args.sortProperties.first.isEmpty());
      sink = new KuduTableSink(table, sinkAction, args.referencedColumns,
          outputExprs, args.kuduTxnToken);
    }

    if (sink == null) {
      throw new UnsupportedOperationException(
          "Cannot create data sink into table of type: " + table.getClass().getName());
    }

    return sink;
  }

  protected ProcessingCost computeDefaultProcessingCost() {
    // TODO: consider including materialization cost into the returned cost.
    return ProcessingCost.basicCost(getLabel(), fragment_.getPlanRoot().getCardinality(),
        ExprUtil.computeExprsTotalCost(outputExprs_));
  }

  /**
   * Arguments for 'create' factory method.
   */
  public static class TableSinkArgs {

    public List<Integer> referencedColumns = Collections.emptyList();
    public boolean overwrite = false;
    public boolean inputIsClustered = false;
    /**
     * Specifies the indices into the list of non-clustering columns of the target
     * table that are stored in the 'sort.columns' table property, and the sorting order.
     */
    public Pair<List<Integer>, TSortingOrder> sortProperties = new Pair<>(
        ImmutableList.of(), TSortingOrder.LEXICAL);
    public long writeId = -1;
    public ByteBuffer kuduTxnToken = null;
    public int maxTableSinks = -1;
    public boolean isResultSink = false;
    public int deleteTableId = 0;

    public static TableSinkArgs withDeleteTableId(int deleteTableId) {
      TableSinkArgs args = new TableSinkArgs();
      args.deleteTableId = deleteTableId;
      return args;
    }

    public static TableSinkArgs withMaxTableSinks(int maxTableSinks) {
      TableSinkArgs args = new TableSinkArgs();
      args.maxTableSinks = maxTableSinks;
      return args;
    }
  }

  public interface HasQuantityLimit {
    int getNumNodes();
    int getNumInstances();
  }
}
