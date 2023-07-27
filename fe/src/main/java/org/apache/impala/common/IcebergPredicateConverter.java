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

package org.apache.impala.common;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.Type;
import org.apache.impala.util.ExprUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergPredicateConverter {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergPredicateConverter.class);
  private final Schema schema_;
  private final Analyzer analyzer_;

  public IcebergPredicateConverter(Schema schema, Analyzer analyzer) {
    this.schema_ = schema;
    this.analyzer_ = analyzer;
  }

  public Expression convert(Expr expr) throws AnalysisException {
    if (expr instanceof BinaryPredicate) {
      return convert((BinaryPredicate) expr);
    } else if (expr instanceof InPredicate) {
      return convert((InPredicate) expr);
    } else if (expr instanceof IsNullPredicate) {
      return convert((IsNullPredicate) expr);
    } else if (expr instanceof CompoundPredicate) {
      return convert((CompoundPredicate) expr);
    } else {
      throw new AnalysisException(String.format(
          "Unsupported expression: %s", expr.toSql()));
    }
  }

  protected Expression convert(BinaryPredicate predicate) throws AnalysisException {
    Term term = getTerm(predicate.getChild(0));
    IcebergColumn column = term.referencedColumn_;

    LiteralExpr literal = getSecondChildAsLiteralExpr(predicate);
    if (Expr.IS_NULL_LITERAL.apply(literal)) {
      throw new AnalysisException("Expression can't be NULL literal: " + literal);
    }
    Operation op = getOperation(predicate);
    Object value = getIcebergValue(column, literal);

    List<Object> literals = Collections.singletonList(value);
    return Expressions.predicate(op, term.term_, literals);
  }

  protected UnboundPredicate<Object> convert(InPredicate predicate)
      throws AnalysisException {
    Term term = getTerm(predicate.getChild(0));
    IcebergColumn column = term.referencedColumn_;
    // Expressions takes a list of values as Objects
    List<Object> values = new ArrayList<>();
    for (int i = 1; i < predicate.getChildren().size(); ++i) {
      if (!Expr.IS_LITERAL.apply(predicate.getChild(i))) {
        throw new AnalysisException(
            String.format("Expression is not a literal: %s",
                predicate.getChild(i)));
      }
      LiteralExpr literal = (LiteralExpr) predicate.getChild(i);
      if (Expr.IS_NULL_LITERAL.apply(literal)) {
        throw new AnalysisException("Expression can't be NULL literal: " + literal);
      }
      Object value = getIcebergValue(column, literal);
      values.add(value);
    }

    // According to the method:
    // 'org.apache.iceberg.expressions.InclusiveMetricsEvaluator.MetricsEvalVisitor#notIn'
    // Expressions.notIn only works when the push-down column is the partition column
    if (predicate.isNotIn()) {
      return Expressions.notIn(term.term_, values);
    } else {
      return Expressions.in(term.term_, values);
    }
  }

  protected UnboundPredicate<Object> convert(IsNullPredicate predicate)
      throws AnalysisException {
    Term term = getTerm(predicate.getChild(0));
    if (predicate.isNotNull()) {
      return Expressions.notNull(term.term_);
    } else {
      return Expressions.isNull(term.term_);
    }
  }

  protected Expression convert(CompoundPredicate predicate) throws AnalysisException {
    Operation op = getOperation(predicate);

    Expr leftExpr = predicate.getChild(0);
    Expression left = convert(leftExpr);

    if (op.equals(Operation.NOT)) {
      return Expressions.not(left);
    }

    Expr rightExpr = predicate.getChild(1);
    Expression right = convert(rightExpr);

    return op.equals(Operation.AND) ? Expressions.and(left, right) :
        Expressions.or(left, right);
  }

  protected Object getIcebergValue(IcebergColumn column, LiteralExpr literal)
      throws AnalysisException {
    switch (literal.getType().getPrimitiveType()) {
      case BOOLEAN: return ((BoolLiteral) literal).getValue();
      case TINYINT:
      case SMALLINT:
      case INT: return ((NumericLiteral) literal).getIntValue();
      case BIGINT: return ((NumericLiteral) literal).getLongValue();
      case FLOAT: return (float) ((NumericLiteral) literal).getDoubleValue();
      case DOUBLE: return ((NumericLiteral) literal).getDoubleValue();
      case STRING:
      case DATETIME:
      case CHAR: return ((StringLiteral) literal).getUnescapedValue();
      case TIMESTAMP: return getIcebergTsValue(literal, column, schema_);
      case DATE: return ((DateLiteral) literal).getValue();
      case DECIMAL: return getIcebergDecimalValue(column, (NumericLiteral) literal);
      default: {
        Preconditions.checkState(false,
            "Unsupported Iceberg type considered for predicate: %s",
            literal.getType().toSql());
      }
    }
    throw new AnalysisException(
        String.format("Unable to parse Iceberg value: %s,",
            literal.getStringValue()));
  }

  /**
   * Returns Iceberg operator by BinaryPredicate operator, or null if the operation is not
   * supported by Iceberg.
   */
  protected Operation getIcebergOperator(BinaryPredicate.Operator op)
      throws AnalysisException {
    switch (op) {
      case EQ: return Operation.EQ;
      case NE: return Operation.NOT_EQ;
      case LE: return Operation.LT_EQ;
      case GE: return Operation.GT_EQ;
      case LT: return Operation.LT;
      case GT: return Operation.GT;
      default:
        throw new AnalysisException(
            String.format("Unsupported Impala operator: %s", op.getName()));
    }
  }

  /**
   * Returns Iceberg operator by CompoundPredicate operator, or null if the operation is
   * not supported by Iceberg.
   */
  protected Operation getIcebergOperator(CompoundPredicate.Operator op)
      throws AnalysisException {
    switch (op) {
      case AND: return Operation.AND;
      case OR: return Operation.OR;
      case NOT: return Operation.NOT;
      default:
        throw new AnalysisException(
            String.format("Unsupported Impala operator: %s", op));
    }
  }

  protected BigDecimal getIcebergDecimalValue(IcebergColumn column,
      NumericLiteral literal) throws AnalysisException {
    Type colType = column.getType();
    int scale = colType.getDecimalDigits();
    BigDecimal literalValue = literal.getValue();

    if (literalValue.scale() > scale) {
      throw new AnalysisException(
          String.format("Invalid scale %d for type: %s", literalValue.scale(),
              colType.toSql()));
    }
    // Iceberg DecimalLiteral needs to have the exact same scale.
    if (literalValue.scale() < scale) {
      return literalValue.setScale(scale);
    }
    return literalValue;
  }

  protected Long getIcebergTsValue(LiteralExpr literal, IcebergColumn column,
      Schema iceSchema) throws AnalysisException {
    try {
      org.apache.iceberg.types.Type iceType = iceSchema.findType(column.getFieldId());
      Preconditions.checkState(iceType instanceof Types.TimestampType);
      Types.TimestampType tsType = (Types.TimestampType) iceType;
      if (tsType.shouldAdjustToUTC()) {
        return ExprUtil.localTimestampToUnixTimeMicros(analyzer_, literal);
      } else {
        return ExprUtil.utcTimestampToUnixTimeMicros(analyzer_, literal);
      }
    } catch (InternalException ex) {
      // We cannot interpret the timestamp literal. Maybe the timestamp is invalid,
      // or the local timestamp ambiguously converts to UTC due to daylight saving
      // time backward turn. E.g. '2021-10-31 02:15:00 Europe/Budapest' converts to
      // either '2021-10-31 00:15:00 UTC' or '2021-10-31 01:15:00 UTC'.
      LOG.warn("Exception occurred during timestamp conversion: %s"
              + "\nThis means timestamp predicate is not pushed to Iceberg, let Impala "
              + "backend handle it.", ex);
    }
    throw new AnalysisException(
        String.format("Unable to parse timestamp value from: %s",
            literal.getStringValue()));
  }

  protected Column getColumnFromSlotRef(SlotRef slotRef) throws AnalysisException {
    SlotDescriptor desc = slotRef.getDesc();
    // If predicate contains map/struct, this column would be null
    Column column = desc.getColumn();
    if (column == null) {
      throw new AnalysisException(
          "Expressions with complex types can't be converted to Iceberg expressions: "
          + slotRef);
    }
    return column;
  }


  protected LiteralExpr getSecondChildAsLiteralExpr(Expr expr) throws AnalysisException {
    if (!(expr.getChild(1) instanceof LiteralExpr)) {
      throw new AnalysisException("Invalid child expression: " + expr);
    }
    return (LiteralExpr) expr.getChild(1);
  }

  protected Operation getOperation(Expr expr) throws AnalysisException {
    Operation op;
    if (expr instanceof BinaryPredicate) {
      op = getIcebergOperator(((BinaryPredicate) expr).getOp());
    } else if (expr instanceof CompoundPredicate) {
      op = getIcebergOperator(((CompoundPredicate) expr).getOp());
    } else {
      throw new AnalysisException("Invalid expression");
    }
    return op;
  }

  protected Term getTerm(Expr expr) throws AnalysisException {
    if(!(expr instanceof SlotRef)){
      throw new AnalysisException(
          String.format("Unable to create term from expression: %s",
              expr.toSql()));
    }
    Column column = getColumnFromSlotRef((SlotRef) expr);
    if(!(column instanceof IcebergColumn)){
      throw new AnalysisException(
          String.format("Invalid column type %s for column: %s",
              column.getType(), column));
    }

    return new Term(Expressions.ref(column.getName()),
        (IcebergColumn) column);

  }
  public static class Term {
    public final UnboundTerm<Object> term_;
    public final IcebergColumn referencedColumn_;

    public Term(UnboundTerm<Object> term, IcebergColumn referencedColumn){
      term_ = term;
      referencedColumn_ = referencedColumn;
    }
  }
}
