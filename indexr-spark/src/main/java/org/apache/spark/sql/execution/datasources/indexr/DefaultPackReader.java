/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.indexr;

import com.google.common.base.Preconditions;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.indexr.io.ByteSlice;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SQLType;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.pack.DataPackNode;
import io.indexr.segment.storage.CachedSegment;
import io.indexr.segment.storage.Version;
import io.indexr.util.DateTimeUtil;
import io.indexr.util.MemoryUtil;
import io.indexr.vlt.segment.pack.DataPackNode_VLT;

class DefaultPackReader implements PackReader {
    private static final int MAX_ROW_COUNT_PER_STEP = DataPack.MAX_COUNT >> 1;
    private static final int MIN_ROW_COUNT_PER_STEP = DataPack.MAX_COUNT >> 4;
    private static final int DEFAULT_STEP_VECTOR_SIZE = 256; // L2 cache size
    private static final int STEP_VECTOR_SIZE;

    static {
        STEP_VECTOR_SIZE = Integer.parseInt(System.getProperty("indexr.vector.size.kb", String.valueOf(DEFAULT_STEP_VECTOR_SIZE))) << 10;
    }

    private static final Logger logger = LoggerFactory.getLogger(DefaultPackReader.class);

    private CachedSegment segment;
    private ColumnSchema[] projectInfos;
    private int[] projColIds;
    private int packId;
    private int packRowCount;
    private int stepRowCount;
    private int rowOffset;

    DefaultPackReader(CachedSegment segment,
                      int packId,
                      ColumnSchema[] projectInfos,
                      int[] projColIds,
                      int maxBatchSize) throws IOException {
        int packRowCount = -1;
        int projectSizePerRow = 0;
        if (projectInfos.length == 0) {
            packRowCount = DataPack.packRowCount(segment.rowCount(), packId);
            projectSizePerRow = 1;
        } else {
            for (int projectId = 0; projectId < projectInfos.length; projectId++) {
                int columnId = projColIds[projectId];

                ColumnSchema cs = projectInfos[projectId];
                Column column = segment.column(columnId);
                DataPackNode dpn = column.dpn(packId);

                assert packRowCount == -1 || packRowCount == dpn.objCount();

                packRowCount = dpn.objCount();
                int packSize;
                if (dpn instanceof DataPackNode_VLT) {
                    packSize = ((DataPackNode_VLT) dpn).dataSize();
                } else {
                    if (column.dataType() == ColumnType.STRING) {
                        // We don't know the exact size.
                        packSize = dpn.packSize() * 5;
                    } else {
                        packSize = ColumnType.bufferSize(column.dataType());
                    }
                }
                projectSizePerRow += Math.ceil(((double) packSize) / packRowCount);
                if (column.dataType() == ColumnType.STRING) {
                    // strings' len varys.
                    projectSizePerRow = (int) (projectSizePerRow * 1.2f);
                }
            }
            projectSizePerRow = Math.max(projectSizePerRow, 1);
        }

        this.projectInfos = projectInfos;
        this.projColIds = projColIds;
        this.segment = segment;
        this.packId = packId;
        this.packRowCount = packRowCount;
        this.stepRowCount = Math.min(Math.min(Math.max(STEP_VECTOR_SIZE / projectSizePerRow, MIN_ROW_COUNT_PER_STEP), MAX_ROW_COUNT_PER_STEP), maxBatchSize);
        this.rowOffset = 0;

        logger.debug(
                "packId:{}, packRowCount:{}, stepRowCount:{}, rowOffset:{}",
                packId, packRowCount, stepRowCount, rowOffset);

        if (!hasMore()) {
            clear();
        }
    }

    @Override
    public void clear() throws IOException {
        if (segment != null) {
            segment.free();
            segment = null;
            packId = -1;
            packRowCount = -1;
            stepRowCount = -1;
            rowOffset = -1;
        }
    }

    @Override
    public boolean hasMore() {
        return segment != null & rowOffset < packRowCount;
    }

    @Override
    public int read(ColumnarBatch columnarBatch) throws IOException {
        Preconditions.checkState(hasMore());

        int offset = rowOffset;
        int count = Math.min(packRowCount - offset, stepRowCount);
        rowOffset = offset + count;
        for (int projectId = 0; projectId < projectInfos.length; projectId++) {
            ColumnSchema columnSchema = projectInfos[projectId];
            int columnId = projColIds[projectId];
            ColumnVector columnVector = columnarBatch.column(projectId);
            DataPack dataPack = segment.column(columnId).pack(packId);
            SQLType sqlType = columnSchema.getSqlType();

            if (!readByCopy(dataPack, columnVector, sqlType, offset, count)) {
                readBySet(dataPack, columnVector, sqlType, offset, count);
            }
        }
        columnarBatch.setNumRows(count);

        if (!hasMore()) {
            clear();
        }
        return count;
    }

    private static boolean readByCopy(DataPack dataPack,
                                      ColumnVector columnVector,
                                      SQLType sqlType,
                                      int offset,
                                      int count) {
        if (dataPack.version() == Version.VERSION_0_ID || !sqlType.isNumber()) {
            // Version_0 does not support.
            return false;
        }
        HackColumnVector hack = HackColumnVector.create(columnVector);
        if (hack == null) {
            return false;
        }
        ByteSlice packData = dataPack.data();
        int byteOffset = offset << ColumnType.numTypeShift(sqlType.dataType);
        switch (sqlType) {
            case INT:
                hack.copyInts(packData.address() + byteOffset, 0, count);
                return true;
            case BIGINT:
                hack.copyLongs(packData.address() + byteOffset, 0, count);
                return true;
            case FLOAT:
                hack.copyFloats(packData.address() + byteOffset, 0, count);
                return true;
            case DOUBLE:
                hack.copyDoubles(packData.address() + byteOffset, 0, count);
                return true;
            default:
                return false;
        }
    }

    private static void readBySet(DataPack dataPack,
                                  ColumnVector columnVector,
                                  SQLType sqlType,
                                  int offset,
                                  int count) {
        switch (sqlType) {
            case INT: {
                dataPack.foreach(offset, count, (int id, int value) -> columnVector.putInt(id - offset, value));
                break;
            }
            case BIGINT: {
                dataPack.foreach(offset, count, (int id, long value) -> columnVector.putLong(id - offset, value));
                break;
            }
            case FLOAT: {
                dataPack.foreach(offset, count, (int id, float value) -> columnVector.putFloat(id - offset, value));
                break;
            }
            case DOUBLE: {
                dataPack.foreach(offset, count, (int id, double value) -> columnVector.putDouble(id - offset, value));
                break;
            }
            case DATE: {
                dataPack.foreach(offset, count, (int id, long value) ->
                        // spark use days, while indexr store millisecond.
                        columnVector.putInt(id - offset, (int) (value / DateTimeUtil.MILLIS_PER_DAY)));
                break;
            }
            case DATETIME: {
                dataPack.foreach(offset, count, (int id, long value) ->
                        // microsecond in spark, indexr only have millisecond.
                        columnVector.putLong(id - offset, value * 1000));
                break;
            }
            case VARCHAR: {
                ByteBuffer byteBuffer = MemoryUtil.getHollowDirectByteBuffer();

                dataPack.foreach(offset, count, (int id, byte[] bytes, int off, int len) ->
                        columnVector.putByteArray(id - offset, bytes, off, len));
                break;
            }
            default:
                throw new IllegalStateException(String.format("Unsupported date type %s", sqlType));
        }
    }
}
