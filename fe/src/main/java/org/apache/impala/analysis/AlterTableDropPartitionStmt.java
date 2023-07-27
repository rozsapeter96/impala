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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;
import java.util.Collections;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableDropPartitionParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TIcebergDropPartitionSummary;


/**
 * Represents an ALTER TABLE DROP PARTITION statement.
 */
public class AlterTableDropPartitionStmt extends AlterTableStmt {

  private final boolean ifExists_;
  private final PartitionSet partitionSet_;

  // Setting this value causes dropped partition(s) to be permanently
  // deleted. For example, for HDFS tables it skips the trash mechanism
  private final boolean purgePartition_;

  public AlterTableDropPartitionStmt(TableName tableName,
      PartitionSet partitionSet, boolean ifExists, boolean purgePartition) {
    super(tableName);
    Preconditions.checkNotNull(partitionSet);
    partitionSet_ = partitionSet;
    partitionSet_.setTableName(tableName);
    ifExists_ = ifExists;
    purgePartition_ = purgePartition;
  }

  public boolean getIfNotExists() { return ifExists_; }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("ALTER TABLE " + getTbl());
    sb.append(" DROP ");
    if (ifExists_) sb.append("IF EXISTS ");
    sb.append(partitionSet_.toSql(options));
    if (purgePartition_) sb.append(" PURGE");
    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.DROP_PARTITION);
    TAlterTableDropPartitionParams addPartParams = new TAlterTableDropPartitionParams();
    addPartParams.setIf_exists(!partitionSet_.getPartitionShouldExist());
    addPartParams.setPurge(purgePartition_);
    params.setDrop_partition_params(addPartParams);
    if (table_ instanceof FeIcebergTable) {
      TIcebergDropPartitionSummary summary = new TIcebergDropPartitionSummary();
      if (partitionSet_.isIcebergTruncate()) {
        summary.setPaths(Collections.emptyList());
        summary.setIs_truncate(true);
      } else {
        summary.setPaths(partitionSet_.getIcebergFiles());
      }
      summary.num_partitions = partitionSet_.getNumberOfDroppedIcebergPartitions();
      addPartParams.setIceberg_drop_partition_summary(summary);
      addPartParams.setPartition_set(Collections.emptyList());
    } else {
      addPartParams.setPartition_set(partitionSet_.toThrift());
    }
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    this.analyzer_ = analyzer;
    super.analyze(analyzer);
    FeTable table = getTargetTable();
    if (table instanceof FeKuduTable) {
      throw new AnalysisException("ALTER TABLE DROP PARTITION is not supported for " +
          "Kudu tables: " + partitionSet_.toSql());
    }
    if (!ifExists_) partitionSet_.setPartitionShouldExist();
    partitionSet_.setPrivilegeRequirement(Privilege.ALTER);
    partitionSet_.analyze(analyzer);

    if (table instanceof FeIcebergTable) {
      analyzeIceberg();
    }
  }

  public void analyzeIceberg() throws AnalysisException {
    if (purgePartition_) {
      throw new AnalysisException(
          "Partition purge is not supported for Iceberg tables");
    }
    if (partitionSet_.getNumberOfDroppedIcebergPartitions() == 0 && !ifExists_) {
      throw new AnalysisException(
          "No matching partition(s) found");
    }
  }
}
