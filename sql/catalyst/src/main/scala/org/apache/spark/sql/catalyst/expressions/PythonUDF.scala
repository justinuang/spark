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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.DataType
import org.apache.spark.{Accumulator}
import java.util.{Map => JMap}
import java.util.{List => JList}
import org.apache.spark.{Accumulator, Logging => SparkLogging}

case class PythonUDF(
                      name: String,
                      command: Array[Byte],
                      envVars: JMap[String, String],
                      pythonIncludes: JList[String],
                      pythonExec: String,
                      pythonVer: String,
                      broadcastVars: JList[Broadcast[PythonBroadcast]],
                      accumulator: Accumulator[JList[Array[Byte]]],
                      dataType: DataType,
                      children: Seq[Expression]
                    ) extends Expression with Unevaluable with SparkLogging {

  override def toString: String = s"PythonUDF#$name(${children.mkString(",")})"

  override def nullable: Boolean = true
}

