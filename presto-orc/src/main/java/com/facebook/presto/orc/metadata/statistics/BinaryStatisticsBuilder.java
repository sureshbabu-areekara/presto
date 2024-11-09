/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.common.block.Block;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class BinaryStatisticsBuilder
        implements SliceColumnStatisticsBuilder
{
    private long nonNullValueCount;
    private long storageSize;
    private long rawSize;
    private long sum;

    @Override
    public void addValue(Block block, int position)
    {
        requireNonNull(block, "block is null");
        sum += block.getSliceLength(position);
        nonNullValueCount++;
    }

    private Optional<BinaryStatistics> buildBinaryStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new BinaryStatistics(sum));
    }

    private void addBinaryStatistics(long valueCount, BinaryStatistics value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount += valueCount;
        sum += value.getSum();
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<BinaryStatistics> binaryStatistics = buildBinaryStatistics();
        if (binaryStatistics.isPresent()) {
            verify(nonNullValueCount > 0);
            return new BinaryColumnStatistics(nonNullValueCount, null, rawSize, storageSize, binaryStatistics.orElseThrow());
        }
        return new ColumnStatistics(nonNullValueCount, null, rawSize, storageSize);
    }

    @Override
    public void incrementRawSize(long rawSize)
    {
        this.rawSize += rawSize;
    }

    @Override
    public void incrementSize(long storageSize)
    {
        this.storageSize += storageSize;
    }

    public static Optional<BinaryStatistics> mergeBinaryStatistics(List<ColumnStatistics> stats)
    {
        BinaryStatisticsBuilder binaryStatisticsBuilder = new BinaryStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            BinaryStatistics partialStatistics = columnStatistics.getBinaryStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                binaryStatisticsBuilder.addBinaryStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return binaryStatisticsBuilder.buildBinaryStatistics();
    }
}
