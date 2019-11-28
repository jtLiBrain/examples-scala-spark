package com.jtLiBrain.examples.spark.sql.udwf

import java.util.UUID

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, If, IsNotNull, LessThanOrEqual, Literal, ScalaUDF, Subtract}
import org.apache.spark.sql.types.{DataType, LongType, StringType}

/**
  * see
  *   1. org/apache/spark/sql/catalyst/expressions/windowExpressions.scala
  *   2. org.apache.spark.sql.catalyst.expressions.ScalaUDFSuite
  *   3. http://blog.nuvola-tech.com/2017/10/spark-custom-window-function-for-sessionization/
  *   4. https://github.com/zheniantoushipashi/spark-udwf-session/blob/master/src/main/scala/com/nuvola_tech/spark/SessionUDWF.scala
  *
  * @param timestampExpr
  * @param sessionExpr
  * @param maxSessionLengthMS
  */
case class SessionUDWF(timestampExpr:Expression, sessionExpr:Expression, maxSessionLengthMS: Long = 3600 * 1000) extends AggregateWindowFunction {self: Product =>
  private val maxSessionDuration:Expression = Literal(maxSessionLengthMS)

  private val zeroTs = Literal.create(0L, LongType)
  private val emptySession = Literal.create(null, StringType)

  private val lastSessionColumn = AttributeReference("currentSessionInBuffer", StringType, true)()
  private val lastTsColumn = AttributeReference("lastTsInBuffer", LongType, false)()

  /**
    * FIXME NOTE:
    * Since createNewSessionUDF, assignSessionExpr and updateExpressions are all fields,
    * so createNewSessionUDF and assignSessionExpr must be coded before updateExpressions
    */
  private val createNewSessionUDF = ScalaUDF(
      () => org.apache.spark.unsafe.types.UTF8String.fromString(UUID.randomUUID().toString),
      StringType,
      Nil,
      Nil
    )

  private val assignSessionExpr = If(
      LessThanOrEqual(Subtract(timestampExpr, aggBufferAttributes(1)), maxSessionDuration), // if timestamp - previousTsColumn <= maxSessionDuration
      aggBufferAttributes(0),                                                               // then use last sessionId
      createNewSessionUDF                                                                   // else create new session
    )


  override def prettyName: String = "make-session-id"

  /**
    * This method is defined in TreeNode
    *
    * Temporarily, I think this field is like a expression-holder for the input
    *
    * @return Returns a seq of the children of this node.
    */
  override def children: Seq[Expression] = sessionExpr :: timestampExpr :: Nil

  /**
    * This method is defined in Expression
    *
    * @return Returns the [[DataType]] of the result of evaluating this expression
    */
  override def dataType: DataType = StringType

  /**
    * This method is defined in AggregateFunction
    *
    * @return Attributes of fields in aggBufferSchema.
    */
  override def aggBufferAttributes: Seq[AttributeReference] = lastSessionColumn :: lastTsColumn :: Nil

  /* followings three fields are defined in DeclarativeAggregate */
  override val initialValues: Seq[Expression] = emptySession :: zeroTs :: Nil
  override val updateExpressions: Seq[Expression] = If(IsNotNull(sessionExpr), sessionExpr, assignSessionExpr) :: timestampExpr :: Nil
  /**
    * An expression which returns the final value for this aggregate function.
    * Its data type should match this expression's [[dataType]].
    */
  override val evaluateExpression: Expression = aggBufferAttributes(0)
}

object SQLUtil {
  val maxSessionLengthMS = 3600 * 1000
  def withExpr(expr: Expression): Column = new Column(expr)

  def calculateSession(ts:Column, sess:Column): Column = withExpr {
    SessionUDWF(ts.expr, sess.expr, maxSessionLengthMS)
  }

  def calculateSession(ts:Column, sess:Column, sessionWindow:Column, maxSessionDuration: Long): Column = withExpr {
    SessionUDWF(ts.expr,sess.expr, maxSessionDuration)
  }
}
