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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IcebergPartitionPredicateConverter;
import org.apache.impala.common.IcebergPredicateConverter;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Reference;
import org.apache.impala.planner.HdfsPartitionPruner;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.util.IcebergUtil;

/**
 * Represents a set of partitions resulting from evaluating a list of partition conjuncts
 * against a table's partition list.
 */
public class PartitionSet extends PartitionSpecBase {
  private final List<Expr> partitionExprs_;

  // Result of analysis, null until analysis is complete.
  private List<? extends FeFsPartition> partitions_;

  // Stores files selected by Iceberg's partition filtering
  private List<String> icebergFilePaths_;
  // Statistics for selected Iceberg partitions
  private long numberOfDroppedIcebergPartitions_;

  // If every partition is selected by Iceberg's partition filtering, this flag signals
  // that a truncate should be executed instead of deleting every file from the metadata.
  private boolean isIcebergTruncate_ = false;

  public PartitionSet(List<Expr> partitionExprs) {
    this.partitionExprs_ = ImmutableList.copyOf(partitionExprs);
  }

  public List<? extends FeFsPartition> getPartitions() { return partitions_; }

  public List<String> getIcebergFiles() { return icebergFilePaths_; }
  public long getNumberOfDroppedIcebergPartitions() {
    return numberOfDroppedIcebergPartitions_;
  }

  public boolean isIcebergTruncate(){
    return isIcebergTruncate_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    if(table_ instanceof IcebergTable){
      analyzeIceberg(analyzer);
      return;
    }
    List<Expr> conjuncts = new ArrayList<>();
    // Do not register column-authorization requests.
    analyzer.setEnablePrivChecks(false);
    for (Expr e : partitionExprs_) {
      e.analyze(analyzer);
      e.checkReturnsBool("Partition expr", false);
      conjuncts.addAll(e.getConjuncts());
    }

    TupleDescriptor desc = analyzer.getDescriptor(tableName_.toString());
    List<SlotId> partitionSlots = desc.getPartitionSlots();
    for (Expr e : conjuncts) {
      analyzer.getConstantFolder().rewrite(e, analyzer);
      // Make sure there are no constant predicates in the partition exprs.
      if (e.isConstant()) {
        throw new AnalysisException(String.format("Invalid partition expr %s. A " +
            "partition spec may not contain constant predicates.", e.toSql()));
      }

      // Make sure every conjunct only contains partition slot refs.
      if (!e.isBoundBySlotIds(partitionSlots)) {
        throw new AnalysisException("Partition exprs cannot contain non-partition " +
            "column(s): " + e.toSql() + ".");
      }
    }

    List<Expr> transformedConjuncts = transformPartitionConjuncts(analyzer, conjuncts);
    addIfExists(analyzer, table_, transformedConjuncts);

    try {
      HdfsPartitionPruner pruner = new HdfsPartitionPruner(desc);
      partitions_ = pruner.prunePartitions(analyzer, transformedConjuncts, true,
          null).first;
    } catch (ImpalaException e) {
      if (e instanceof AnalysisException) { throw(AnalysisException) e; }
      throw new AnalysisException("Partition expr evaluation failed in the backend.", e);
    }

    if (partitionShouldExist_ != null) {
      if (partitionShouldExist_ && partitions_.isEmpty()) {
        throw new AnalysisException("No matching partition(s) found.");
      } else if (!partitionShouldExist_ && !partitions_.isEmpty()) {
        throw new AnalysisException("Partition already exists.");
      }
    }
    analyzer.setEnablePrivChecks(true);
  }

  public void analyzeIceberg(Analyzer analyzer) throws AnalysisException {
    FeIcebergTable table = (FeIcebergTable) table_;

    IcebergPartitionExpressionRewriter rewriter =
        new IcebergPartitionExpressionRewriter(analyzer);
    IcebergPredicateConverter converter =
        new IcebergPartitionPredicateConverter(table.getIcebergSchema(), analyzer);

    List<Expression> icebergExpressions = new ArrayList<>();
    for (Expr expr : partitionExprs_) {
      expr = rewriter.rewriteReferences(expr);
      expr.analyze(analyzer);
      analyzer.getConstantFolder().rewrite(expr, analyzer);
      try {
        icebergExpressions.add(converter.convert(expr));
      }
      catch (ImpalaException e) {
        throw new AnalysisException(
            "Invalid partition filtering expression: " + expr.toSql());
      }
    }

    try (CloseableIterable<FileScanTask> fileScanTasks = IcebergUtil.planFiles(table,
        icebergExpressions, null)) {
      GroupedContentFiles icebergFiles = new GroupedContentFiles();
      HashMap<String, Long> icebergPartitionSummary = new HashMap<>();
      for (FileScanTask fileScanTask : fileScanTasks) {
        if (fileScanTask.residual().isEquivalentTo(Expressions.alwaysTrue())) {
          icebergPartitionSummary.merge(fileScanTask.file().partition().toString(), 1L,
              Long::sum);
          if (fileScanTask.deletes().isEmpty()) {
            icebergFiles.dataFilesWithoutDeletes.add(fileScanTask.file());
          } else {
            icebergFiles.dataFilesWithDeletes.add(fileScanTask.file());
            icebergFiles.deleteFiles.addAll(fileScanTask.deletes());
          }
        }
      }
      numberOfDroppedIcebergPartitions_ = icebergPartitionSummary.size();
      if (icebergFiles.size() == FeIcebergTable.Utils.getTotalNumberOfFiles(table,
          null)) {
        isIcebergTruncate_ = true;
        return;
      }
      icebergFilePaths_ = new ArrayList<>(icebergFiles.size());
      for(ContentFile<?> contentFile : icebergFiles.getAllContentFiles()){
        icebergFilePaths_.add(contentFile.path().toString());
      }
    } catch (IOException | TableLoadingException | ImpalaRuntimeException e) {
      throw new AnalysisException("Error loading metadata for Iceberg table", e);
    }
  }

  private class IcebergPartitionExpressionRewriter {
    private final Analyzer analyzer_;
    public IcebergPartitionExpressionRewriter(Analyzer analyzer){
      this.analyzer_ = analyzer;
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
    public Expr rewriteReferences(Expr expr) throws AnalysisException {
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
      if (!(literal instanceof StringLiteral))  return;
      if (!IcebergUtil.isDateTimeTransformType(transformType)) return;
      try {
        Integer dateTimeTransformValue = IcebergUtil.getDateTimeTransformValue(transformType,
            literal.getStringValue());
        rewrite.apply(NumericLiteral.create(dateTimeTransformValue));
      }
      catch (ImpalaRuntimeException ignore){}
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
      org.apache.iceberg.PartitionSpec spec = ((IcebergTable) table_).getIcebergApiTable().spec();
      return new IcebergPartitionExpr(slotRef, spec);
    }

    private IcebergPartitionExpr rewriteReferences(FunctionCallExpr functionCallExpr)
        throws AnalysisException {
      org.apache.iceberg.PartitionSpec spec = ((IcebergTable) table_).getIcebergApiTable().spec();
      return new IcebergPartitionExpr(functionCallExpr, spec);
    }

    private IsNullPredicate rewriteReferences(IsNullPredicate isNullPredicate)
        throws AnalysisException {
      Expr child = isNullPredicate.getChild(0);
      PartitionSpec spec = ((IcebergTable) table_).getIcebergApiTable().spec();

      if (child instanceof SlotRef) {
        isNullPredicate.getChildren().set(0, rewriteReferences(child));
      }
      if (child instanceof FunctionCallExpr) {
        isNullPredicate.getChildren()
            .set(0, new IcebergPartitionExpr((FunctionCallExpr) child, spec));
      }
      return isNullPredicate;
    }
  }

  // Check if we should add IF EXISTS. Fully-specified partition specs don't add it for
  // backwards compatibility, while more general partition expressions or partially
  // specified partition specs add IF EXISTS by setting partitionShouldExists_ to null.
  // The given conjuncts are assumed to only reference partition columns.
  private void addIfExists(
      Analyzer analyzer, FeTable table, List<Expr> conjuncts) {
    boolean add = false;
    Set<String> partColNames = new HashSet<>();
    Reference<SlotRef> slotRef = new Reference<>();
    for (Expr e : conjuncts) {
      if (e instanceof BinaryPredicate) {
        BinaryPredicate bp = (BinaryPredicate) e;
        if (bp.getOp() != Operator.EQ || !bp.isSingleColumnPredicate(slotRef, null)) {
          add = true;
          break;
        }
        Column partColumn = slotRef.getRef().getDesc().getColumn();
        Preconditions.checkState(table.isClusteringColumn(partColumn));
        partColNames.add(partColumn.getName());
      } else if (e instanceof IsNullPredicate) {
        IsNullPredicate nullPredicate = (IsNullPredicate) e;
        Column partColumn = nullPredicate.getBoundSlot().getDesc().getColumn();
        Preconditions.checkState(table.isClusteringColumn(partColumn));
        partColNames.add(partColumn.getName());
      } else {
        add = true;
        break;
      }
    }

    if (add || partColNames.size() < table.getNumClusteringCols()) {
      partitionShouldExist_ = null;
    }
  }

  // Transform <COL> = NULL into IsNull expr; <String COL> = '' into IsNull expr.
  // The reason is that COL = NULL is allowed for selecting the NULL
  // partition, but a COL = NULL predicate can never be true, so we
  // need to transform such predicates before feeding them into the
  // partition pruner.
  private List<Expr> transformPartitionConjuncts(Analyzer analyzer, List<Expr> conjuncts)
      throws AnalysisException {
    List<Expr> transformedConjuncts = new ArrayList<>();
    for (Expr e : conjuncts) {
      Expr result = e;
      if (e instanceof BinaryPredicate) {
        BinaryPredicate bp = ((BinaryPredicate) e);
        if (bp.getOp() == Operator.EQ) {
          SlotRef leftChild =
              bp.getChild(0) instanceof SlotRef ? ((SlotRef) bp.getChild(0)) : null;
          NullLiteral nullChild = Expr.IS_NULL_LITERAL.apply(bp.getChild(1)) ?
              ((NullLiteral) bp.getChild(1)) : null;
          StringLiteral stringChild = bp.getChild(1) instanceof StringLiteral ?
              ((StringLiteral) bp.getChild(1)) : null;
          if (leftChild != null && nullChild != null) {
            result = new IsNullPredicate(leftChild, false);
          } else if (leftChild != null && stringChild != null) {
            if (stringChild.getStringValue().isEmpty()) {
              result = new IsNullPredicate(leftChild, false);
            }
          }
        }
      }
      result.analyze(analyzer);
      transformedConjuncts.add(result);
    }
    return transformedConjuncts;
  }

  public List<List<TPartitionKeyValue>> toThrift() {
    List<List<TPartitionKeyValue>> thriftPartitionSet = new ArrayList<>();
    for (FeFsPartition hdfsPartition : partitions_) {
      List<TPartitionKeyValue> thriftPartitionSpec = new ArrayList<>();
      for (int i = 0; i < table_.getNumClusteringCols(); ++i) {
        String key = table_.getColumns().get(i).getName();
        String value = PartitionKeyValue.getPartitionKeyValueString(
            hdfsPartition.getPartitionValue(i), nullPartitionKeyValue_);
        thriftPartitionSpec.add(new TPartitionKeyValue(key, value));
      }
      thriftPartitionSet.add(thriftPartitionSpec);
    }
    return thriftPartitionSet;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    List<String> partitionExprStr = new ArrayList<>();
    for (Expr e : partitionExprs_) {
      partitionExprStr.add(e.toSql(options));
    }
    return String.format("PARTITION (%s)", Joiner.on(", ").join(partitionExprStr));
  }
}
