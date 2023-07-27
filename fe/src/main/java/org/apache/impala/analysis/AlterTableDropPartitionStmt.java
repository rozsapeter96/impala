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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeIcebergTable.Utils;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IcebergPartitionPredicateConverter;
import org.apache.impala.thrift.TAlterTableDropPartitionParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TIcebergDropPartitionSummary;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.util.IcebergUtil;

/**
 * Represents an ALTER TABLE DROP PARTITION statement.
 */
public class AlterTableDropPartitionStmt extends AlterTableStmt {

  private final boolean ifExists_;
  private final PartitionSet partitionSet_;

  // Setting this value causes dropped partition(s) to be permanently
  // deleted. For example, for HDFS tables it skips the trash mechanism
  private final boolean purgePartition_;

  // Stores files selected by Iceberg's partition filtering
  private IcebergContentFileStore icebergContentFileStore_;
  // Statistics for selected Iceberg partitions
  private Map<String, Long> icebergPartitionSummary_;

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
    if (ifExists_) { sb.append("IF EXISTS "); }
    sb.append(partitionSet_.toSql(options));
    if (purgePartition_) { sb.append(" PURGE"); }
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
      addPartParams.setIceberg_drop_partition_summary(
          new TIcebergDropPartitionSummary(icebergContentFileStore_.toThrift(),
              icebergPartitionSummary_));
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
    if (!ifExists_) { partitionSet_.setPartitionShouldExist(); }
    partitionSet_.setPrivilegeRequirement(Privilege.ALTER);
    if (table instanceof FeIcebergTable) {
      analyzeIceberg(analyzer);
    } else {
      partitionSet_.analyze(analyzer);
    }
  }

  public void analyzeIceberg(Analyzer analyzer) throws AnalysisException {
    if (purgePartition_) {
      throw new AnalysisException(
          "Partition purge is not supported for Iceberg tables");
    }
    List<Expr> expressions = partitionSet_.getExprs();
    FeIcebergTable table = (FeIcebergTable) table_;

    IcebergPartitionPredicateConverter converter =
        new IcebergPartitionPredicateConverter(table.getIcebergSchema(), analyzer);

    List<Expression> icebergExpressions = new ArrayList<>();
    for (Expr expr : expressions) {
      expr = rewriteReferences(expr);
      expr.analyze(analyzer);
      analyzer.getConstantFolder().rewrite(expr, analyzer);
      try {
        Expression icebergExpression = converter.convert(expr);
        icebergExpressions.add(icebergExpression);
      }
      catch (AnalysisException e) {
        throw new AnalysisException(
            "Invalid partition filtering expression: " + expr.toSql());
      }
    }

    GroupedContentFiles icebergFiles;
    icebergPartitionSummary_ = new HashMap<>();
    try (CloseableIterable<FileScanTask> fileScanTasks = IcebergUtil.planFiles(table,
        icebergExpressions, null)) {
      icebergFiles = new GroupedContentFiles();
      for (FileScanTask fileScanTask : fileScanTasks) {
        if (fileScanTask.residual().isEquivalentTo(Expressions.alwaysTrue())) {
          icebergPartitionSummary_.merge(fileScanTask.file().partition().toString(), 1L,
              Long::sum);
          if (fileScanTask.deletes().isEmpty()) {
            icebergFiles.dataFilesWithoutDeletes.add(fileScanTask.file());
          } else {
            icebergFiles.dataFilesWithDeletes.add(fileScanTask.file());
            icebergFiles.deleteFiles.addAll(fileScanTask.deletes());
          }
        }
      }
      if (icebergFiles.isEmpty() && !ifExists_) {
        throw new AnalysisException(
            "No matching partition found for Iceberg expression list: "
                + icebergExpressions);
      }
      icebergContentFileStore_ = Utils.loadAllPartition(
          (IcebergTable) table, icebergFiles);
    } catch (IOException | TableLoadingException e) {
      throw new AnalysisException("Error loading metadata for Iceberg table", e);
    }
  }

  /**
   * Rewrites SlotRefs and FunctionCallExprs as IcebergPartitionExpr. SlotRefs targeting a
   * columns are rewritten to an IcebergPartitionExpr where the transform type is
   * IDENTITY. FunctionExrps are checked whether the function name matches any Iceberg
   * transform name, if it matches, then it gets rewritten to an IcebergPartitionExpr
   * where the transform is located from the function name, and the parameter (if there's
   * any) for the transform is saved as well, and the targeted column's SlotRef is also
   * saved in the IcebergPartitionExpr. The resulting IcebergPartitionExpr then replaces
   * the original SlotRef/FunctionCallExpr. For Date transforms (year, month, day, hour),
   * an implicit conversion happens during the rewriting: string literals formed as
   * Iceberg partition values like "2023", "2023-12", ... are parsed and transformed
   * automatically to their numeric counterparts.
   *
   * @param expr incoming expression tree
   * @return Expr where SlotRefs and FunctionCallExprs are replaced with
   * IcebergPartitionExpr
   * @throws AnalysisException when expression rewrite fails
   */
  private Expr rewriteReferences(Expr expr) throws AnalysisException {
    if (expr instanceof BinaryPredicate) {
      BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
      return rewriteReferences(binaryPredicate);
    }
    if (expr instanceof CompoundPredicate) {
      CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
      return rewriteReferences(compoundPredicate);
    }
    if (expr instanceof SlotRef) {
      SlotRef slotRef = (SlotRef) expr;
      return rewriteReferences(slotRef);
    }
    if (expr instanceof FunctionCallExpr) {
      FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
      return rewriteReferences(functionCallExpr);
    }
    if (expr instanceof IsNullPredicate) {
      IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
      return rewriteReferences(isNullPredicate);
    }
    if (expr instanceof InPredicate) {
      InPredicate isNullPredicate = (InPredicate) expr;
      return rewriteReferences(isNullPredicate);
    }
    throw new AnalysisException(
        "Invalid partition filtering expression: " + expr.toSql());
  }

  private BinaryPredicate rewriteReferences(BinaryPredicate binaryPredicate)
      throws AnalysisException {
    Expr term = binaryPredicate.getChild(0);
    Expr literal = binaryPredicate.getChild(1);
    IcebergPartitionExpr partitionExpr;
    if (term instanceof SlotRef) {
      partitionExpr = rewriteReferences((SlotRef) term);
      binaryPredicate.getChildren().set(0, partitionExpr);
    } else if (term instanceof FunctionCallExpr) {
      partitionExpr = rewriteReferences((FunctionCallExpr) term);
      binaryPredicate.getChildren().set(0, partitionExpr);
    } else {
      return binaryPredicate;
    }
    TIcebergPartitionTransformType transformType = partitionExpr.getTransform()
        .getTransformType();
    if(!(literal instanceof LiteralExpr)){
      return binaryPredicate;
    }
    rewriteDateTransformConstants((LiteralExpr) literal, transformType,
        numericLiteral -> binaryPredicate.getChildren().set(1, numericLiteral));
    return binaryPredicate;
  }

  private InPredicate rewriteReferences(InPredicate inPredicate)
      throws AnalysisException {
    Expr term = inPredicate.getChild(0);
    List<Expr> literals = inPredicate.getChildren()
        .subList(1, inPredicate.getChildCount());
    IcebergPartitionExpr partitionExpr;
    if (term instanceof SlotRef) {
      partitionExpr = rewriteReferences((SlotRef) term);
      inPredicate.getChildren().set(0, partitionExpr);
    } else if (term instanceof FunctionCallExpr) {
      partitionExpr = rewriteReferences((FunctionCallExpr) term);
      inPredicate.getChildren().set(0, partitionExpr);
    } else {
      return inPredicate;
    }
    TIcebergPartitionTransformType transformType = partitionExpr.getTransform()
        .getTransformType();
    for (int i = 0; i < literals.size(); ++i) {
      if (!(literals.get(i) instanceof LiteralExpr)) {
        return inPredicate;
      }
      LiteralExpr literal = (LiteralExpr) literals.get(i);
      int affectedChildId = i + 1;
      rewriteDateTransformConstants(literal, transformType,
          numericLiteral -> inPredicate.getChildren()
              .set(affectedChildId, numericLiteral));
    }
    return inPredicate;
  }

  private void rewriteDateTransformConstants(LiteralExpr literal,
      TIcebergPartitionTransformType transformType,
      Function<NumericLiteral, ?> rewrite) {
    if (transformType.equals(TIcebergPartitionTransformType.YEAR)
        && literal instanceof NumericLiteral) {
      NumericLiteral numericLiteral = (NumericLiteral) literal;
      long longValue = numericLiteral.getLongValue();
      long target = longValue + IcebergUtil.ICEBERG_EPOCH_YEAR;
      analyzer_.addWarning(String.format(
          "The YEAR transform expects years normalized to %d: %d is targeting year %d",
          IcebergUtil.ICEBERG_EPOCH_YEAR, longValue, target));
      return;
    }
    if (!(literal instanceof StringLiteral)) {
      return;
    }
    if (!IcebergUtil.isDateTransformType(transformType)) {
      return;
    }
    IcebergUtil.getDateTransformValue(
        transformType, literal.getStringValue()).ifPresent(
        integer -> rewrite.apply(NumericLiteral.create(integer)));
  }

  private CompoundPredicate rewriteReferences(CompoundPredicate compoundPredicate)
      throws AnalysisException {
    Expr left = compoundPredicate.getChild(0);
    Expr right = compoundPredicate.getChild(1);
    compoundPredicate.setChild(0, rewriteReferences(left));
    compoundPredicate.setChild(1, rewriteReferences(right));
    return compoundPredicate;
  }

  private IcebergPartitionExpr rewriteReferences(SlotRef slotRef) {
    return new IcebergPartitionExpr(slotRef, (IcebergTable) table_);
  }

  private IcebergPartitionExpr rewriteReferences(FunctionCallExpr functionCallExpr)
      throws AnalysisException {
    return new IcebergPartitionExpr(functionCallExpr, (IcebergTable) table_);
  }

  private IsNullPredicate rewriteReferences(IsNullPredicate isNullPredicate)
      throws AnalysisException {
    Expr child = isNullPredicate.getChild(0);
    if (child instanceof SlotRef) {
      isNullPredicate.getChildren().set(0, rewriteReferences(child));
    }
    if (child instanceof FunctionCallExpr) {
      isNullPredicate.getChildren().set(
          0, new IcebergPartitionExpr((FunctionCallExpr) child, (IcebergTable) table_));
    }
    return isNullPredicate;
  }
}
