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

package org.apache.spark.sql.execution.datasources.indexr

import java.net.URI

import io.indexr.segment.SegmentMode
import io.indexr.segment.rc.RCOperator
import io.indexr.util.JsonUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.indexr.IndexRUtil.SchemaStruct
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConversions._

class IndexRFileFormat
  extends FileFormat
    with DataSourceRegister
    with Serializable {

  override def shortName(): String = "indexr"

  override def toString: String = "IndexR"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[IndexRFileFormat]

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    val schemaStruct: SchemaStruct = IndexRUtil.getSchemaStruct(dataSchema, options)
    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new IndexROutputWriter(
          new Path(path),
          context.getConfiguration,
          schemaStruct.schema,
          SegmentMode.fromName(schemaStruct.mode),
          schemaStruct.aggSchema)
      }

      override def getFileExtension(context: TaskAttemptContext): String = ".indexr"
    }
  }

  def inferSchema(
                   sparkSession: SparkSession,
                   parameters: Map[String, String],
                   files: Seq[FileStatus]): Option[StructType] = {
    Option(StructType(IndexRUtil.inferSchema(files, sparkSession.sessionState.newHadoopConf())))
  }

  /**
    * Returns whether the reader will return the rows as batch or not.
    */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val conf = sparkSession.sessionState.conf
    conf.wholeStageEnabled &&
      schema.length <= conf.wholeStageMaxNumFields &&
      schema.forall(_.dataType.isInstanceOf[AtomicType])
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    false
  }

  override def buildReaderWithPartitionValues(
                                               sparkSession: SparkSession,
                                               dataSchema: StructType,
                                               partitionSchema: StructType,
                                               requiredSchema: StructType,
                                               filters: Seq[Filter],
                                               options: Map[String, String],
                                               hadoopConf: Configuration):
  (PartitionedFile) => Iterator[InternalRow] = {
    // For Parquet data source, `buildReader` already handles partition values appending. Here we
    // simply delegate to `buildReader`.
    buildReader(
      sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
  }

  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    // TODO enable filter pushdown

    hadoopConf.set(Config.SPARK_PROJECT_SCHEMA, requiredSchema.json)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val returningBatch = supportBatch(sparkSession, resultSchema)
    val indexrRequiredSchema = IndexRUtil.sparkSchemaToIndexRSchema(requiredSchema)
    val indexrFilterStr = JsonUtil.toJson(SparkFilter.transform(indexrRequiredSchema.getColumns, filters))

    (file: PartitionedFile) => {
      assert(file.partitionValues.numFields == partitionSchema.size)
      assert(file.start == 0)

      val fileSplit =
        new FileSplit(new Path(new URI(file.filePath)), file.start, file.length, file.locations)

      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)

      val hadoopAttemptContext =
        new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

      val reader = new IndexRRecordReader()
      reader.initialize(fileSplit, hadoopAttemptContext)
      val indexrFilter = JsonUtil.fromJson(indexrFilterStr, classOf[RCOperator])
      reader.init2(partitionSchema, file.partitionValues, returningBatch, indexrFilter)

      val iter = new RecordReaderIterator(reader)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => iter.close()))
      iter.asInstanceOf[Iterator[InternalRow]]
    }
  }
}