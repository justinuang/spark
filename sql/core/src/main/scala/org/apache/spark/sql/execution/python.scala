/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.sql.execution

import java.io.OutputStream
import java.util.{List => JList, Map => JMap}

import net.razorvine.pickle._
import org.apache.spark.api.python.{PythonRDD, PythonRunner, SerDeUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{EvaluatePython, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{TaskContext}

import scala.collection.JavaConverters._


/**
 * Extracts PythonUDFs from operators, rewriting the query plan so that the UDF can be evaluated
 * alone in a batch.
 *
 * This has the limitation that the input to the Python UDF is not allowed include attributes from
 * multiple child operators.
 */
private[spark] object ExtractPythonUDFs extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // Skip EvaluatePython nodes.
    case plan: EvaluatePython => plan

    case plan: LogicalPlan if plan.resolved =>
      // Extract any PythonUDFs from the current operator.
      val udfs = plan.expressions.flatMap(_.collect { case udf: PythonUDF => udf })
      if (udfs.isEmpty) {
        // If there aren't any, we are done.
        plan
      } else {
        // Pick the UDF we are going to evaluate (TODO: Support evaluating multiple UDFs at a time)
        // If there is more than one, we will add another evaluation operator in a subsequent pass.
        udfs.find { udf =>
          udf.resolved && udf.children.map { child: Expression =>
            child.find {
              case p: PythonUDF => true
              case _ => false
            }.isEmpty
          }.reduce((x, y) => x && y)
        } match {
          case Some(udf) =>
            var evaluation: EvaluatePython = null

            // Rewrite the child that has the input required for the UDF
            val newChildren = plan.children.map { child =>
              // Check to make sure that the UDF can be evaluated with only the input of this child.
              // Other cases are disallowed as they are ambiguous or would require a cartesian
              // product.
              if (udf.references.subsetOf(child.outputSet)) {
                evaluation = EvaluatePythonUtils(udf, child)
                evaluation
              } else if (udf.references.intersect(child.outputSet).nonEmpty) {
                sys.error(s"Invalid PythonUDF $udf, requires attributes from more than one child.")
              } else {
                child
              }
            }

            assert(evaluation != null, "Unable to evaluate PythonUDF.  Missing input attributes.")

            // Trim away the new UDF value if it was only used for filtering or something.
            logical.Project(
              plan.output,
              plan.transformExpressions {
                case p: PythonUDF if p.fastEquals(udf) => evaluation.resultAttribute(0)
              }.withNewChildren(newChildren)
            )


          case None =>
            // If there is no Python UDF that is resolved, skip this round.
            plan
        }
      }
  }
}

object EvaluatePythonUtils {
  def apply(udf: PythonUDF, child: LogicalPlan): EvaluatePython =
    new EvaluatePython(Seq(udf), child, Seq(AttributeReference("pythonUDF", udf.dataType)()))

  def takeAndServe(df: DataFrame, n: Int): Int = {
    registerPicklers()
    df.withNewExecutionId {
      val iter = new SerDeUtil.AutoBatchedPickler(
        df.queryExecution.executedPlan.executeTake(n).iterator.map { row =>
          EvaluatePythonUtils.toJava(row, df.schema)
        })
      PythonRDD.serveIterator(iter, s"serve-DataFrame")
    }
  }

  /**
   * Helper for converting from Catalyst type to java type suitable for Pyrolite.
   */
  def toJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (row: InternalRow, struct: StructType) =>
      val values = new Array[Any](row.numFields)
      var i = 0
      while (i < row.numFields) {
        values(i) = toJava(row.get(i, struct.fields(i).dataType), struct.fields(i).dataType)
        i += 1
      }
      new GenericInternalRowWithSchema(values, struct)

    case (a: ArrayData, array: ArrayType) =>
      val values = new java.util.ArrayList[Any](a.numElements())
      a.foreach(array.elementType, (_, e) => {
        values.add(toJava(e, array.elementType))
      })
      values

    case (map: MapData, mt: MapType) =>
      val jmap = new java.util.HashMap[Any, Any](map.numElements())
      map.foreach(mt.keyType, mt.valueType, (k, v) => {
        jmap.put(toJava(k, mt.keyType), toJava(v, mt.valueType))
      })
      jmap

    case (ud, udt: UserDefinedType[_]) => toJava(ud, udt.sqlType)

    case (d: Decimal, _) => d.toJavaBigDecimal

    case (s: UTF8String, StringType) => s.toString

    case (other, _) => other
  }

  /**
   * Converts `obj` to the type specified by the data type, or returns null if the type of obj is
   * unexpected. Because Python doesn't enforce the type.
   */
  def fromJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (c: Boolean, BooleanType) => c

    case (c: Int, ByteType) => c.toByte
    case (c: Long, ByteType) => c.toByte

    case (c: Int, ShortType) => c.toShort
    case (c: Long, ShortType) => c.toShort

    case (c: Int, IntegerType) => c
    case (c: Long, IntegerType) => c.toInt

    case (c: Int, LongType) => c.toLong
    case (c: Long, LongType) => c

    case (c: Double, FloatType) => c.toFloat

    case (c: Double, DoubleType) => c

    case (c: java.math.BigDecimal, dt: DecimalType) => Decimal(c, dt.precision, dt.scale)

    case (c: Int, DateType) => c

    case (c: Long, TimestampType) => c

    case (c: String, StringType) => UTF8String.fromString(c)
    case (c, StringType) =>
      // If we get here, c is not a string. Call toString on it.
      UTF8String.fromString(c.toString)

    case (c: String, BinaryType) => c.getBytes("utf-8")
    case (c, BinaryType) if c.getClass.isArray && c.getClass.getComponentType.getName == "byte" => c

    case (c: java.util.List[_], ArrayType(elementType, _)) =>
      new GenericArrayData(c.asScala.map { e => fromJava(e, elementType)}.toArray)

    case (c, ArrayType(elementType, _)) if c.getClass.isArray =>
      new GenericArrayData(c.asInstanceOf[Array[_]].map(e => fromJava(e, elementType)))

    case (c: java.util.Map[_, _], MapType(keyType, valueType, _)) =>
      val keyValues = c.asScala.toSeq
      val keys = keyValues.map(kv => fromJava(kv._1, keyType)).toArray
      val values = keyValues.map(kv => fromJava(kv._2, valueType)).toArray
      ArrayBasedMapData(keys, values)

    case (c, StructType(fields)) if c.getClass.isArray =>
      new GenericInternalRow(c.asInstanceOf[Array[_]].zip(fields).map {
        case (e, f) => fromJava(e, f.dataType)
      })

    case (_, udt: UserDefinedType[_]) => fromJava(obj, udt.sqlType)

    // all other unexpected type should be null, or we will have runtime exception
    // TODO(davies): we could improve this by try to cast the object to expected type
    case (c, _) => null
  }


  private val module = "pyspark.sql.types"

  /**
   * Pickler for StructType
   */
  private class StructTypePickler extends IObjectPickler {

    private val cls = classOf[StructType]

    def register(): Unit = {
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      out.write(Opcodes.GLOBAL)
      out.write((module + "\n" + "_parse_datatype_json_string" + "\n").getBytes("utf-8"))
      val schema = obj.asInstanceOf[StructType]
      pickler.save(schema.json)
      out.write(Opcodes.TUPLE1)
      out.write(Opcodes.REDUCE)
    }
  }

  /**
   * Pickler for InternalRow
   */
  private class RowPickler extends IObjectPickler {

    private val cls = classOf[GenericInternalRowWithSchema]

    // register this to Pickler and Unpickler
    def register(): Unit = {
      Pickler.registerCustomPickler(this.getClass, this)
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write((module + "\n" + "_create_row_inbound_converter" + "\n").getBytes("utf-8"))
      } else {
        // it will be memorized by Pickler to save some bytes
        pickler.save(this)
        val row = obj.asInstanceOf[GenericInternalRowWithSchema]
        // schema should always be same object for memoization
        pickler.save(row.schema)
        out.write(Opcodes.TUPLE1)
        out.write(Opcodes.REDUCE)

        out.write(Opcodes.MARK)
        var i = 0
        while (i < row.values.size) {
          pickler.save(row.values(i))
          i += 1
        }
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
      }
    }
  }

  private[this] var registered = false
  /**
   * This should be called before trying to serialize any above classes un cluster mode,
   * this should be put in the closure
   */
  def registerPicklers(): Unit = {
    synchronized {
      if (!registered) {
        SerDeUtil.initialize()
        new StructTypePickler().register()
        new RowPickler().register()
        registered = true
      }
    }
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   */
  def javaToPython(rdd: RDD[Any]): RDD[Array[Byte]] = {
    rdd.mapPartitions { iter =>
      registerPicklers()  // let it called in executor
      new SerDeUtil.AutoBatchedPickler(iter)
    }
  }
}

/**
 * Uses PythonRDD to evaluate a [[PythonUDF]], one partition of tuples at a time.
 *
 * Python evaluation works by sending the necessary (projected) input data via a socket to an
 * external Python process, and combine the result from the Python process with the original row.
 *
 * For each row we send to Python, we also put it in a queue. For each output row from Python,
 * we drain the queue to find the original input row. Note that if the Python process is way too
 * slow, this could lead to the queue growing unbounded and eventually run out of memory.
 */
case class BatchPythonEvaluation(udfs: Seq[PythonUDF], output: Seq[Attribute], child: SparkPlan)
  extends SparkPlan {

  def children: Seq[SparkPlan] = child :: Nil

  override def outputsUnsafeRows: Boolean = false
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = true

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)

    inputRDD.mapPartitions { iter =>
      EvaluatePythonUtils.registerPicklers()  // register pickler for Row

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = new java.util.concurrent.ConcurrentLinkedQueue[InternalRow]()

      val pickle = new Pickler

      val test_func = (row: InternalRow) => {
        val currentRows = udfs.map { udf =>
          val currentRow = newMutableProjection(udf.children, child.output)()

          currentRow(row)
        }

        val schemas = udfs.map { udf =>
          val fields = udf.children.map(_.dataType)
          val schema = new StructType(fields.map(t => new StructField("", t, true)).toArray)
          schema
        }

        val outerSchema = StructType(schemas.map{  StructField("myColName", _)})

        val out = EvaluatePythonUtils.toJava(InternalRow.fromSeq(currentRows), outerSchema)
        out
      }

      // Input iterator to Python: input rows are grouped so we send them in batches to Python.
      // For each row, add it to the queue.
      val inputIterator = iter.grouped(100).map { inputRows =>
        val toBePickled = inputRows.map { row =>
          queue.add(row)
          test_func(row) // TODO this is a list. fields are duplicated
        }.toArray
        pickle.dumps(toBePickled)
      }

      val context = TaskContext.get()

      val firstUdf = udfs(0)

      // Output iterator for results from Python.
      val outputIterator = new PythonRunner(
        udfs.map{_.command},
        firstUdf.envVars,
        firstUdf.pythonIncludes,
        firstUdf.pythonExec,
        firstUdf.pythonVer,
        firstUdf.broadcastVars,
        firstUdf.accumulator,
        bufferSize,
        reuseWorker
      ).compute(inputIterator, context.partitionId(), context)

      val unpickle = new Unpickler
      val joined = new JoinedRow

      outputIterator.flatMap { pickedResult =>
        val unpickledBatch = unpickle.loads(pickedResult)
        unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
      }.map { result =>
        val compositeDataType = StructType(udfs.map{ (udf) => StructField("a", udf.dataType)})
        val row: InternalRow = EvaluatePythonUtils.fromJava(
          result,
          compositeDataType
        ).asInstanceOf[InternalRow]
        joined(queue.poll(), row)
      }
    }
  }
}
