/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator.impl.stats;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;
import com.linkedin.pinot.core.segment.creator.SegmentPreIndexStatsCollector;

/**
 * Nov 7, 2014
 */

public class SegmentPreIndexStatsCollectorImpl implements SegmentPreIndexStatsCollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreIndexStatsCollectorImpl.class);

  private final Schema dataSchema;
  Map<String, AbstractColumnStatisticsCollector> columnStatsCollectorMap;

  public SegmentPreIndexStatsCollectorImpl(Schema dataSchema) {
    this.dataSchema = dataSchema;
  }

  @Override
  public void init() {
    columnStatsCollectorMap = new HashMap<String, AbstractColumnStatisticsCollector>();

    for (final FieldSpec spec : dataSchema.getAllFieldSpecs()) {
      DataType dataType = spec.getDataType();
      String specName = spec.getName();
      LOGGER.debug("FieldSpec {} : {}", specName, spec);
      if (spec instanceof TimeFieldSpec) {
        TimeFieldSpec timeFieldSpec = (TimeFieldSpec) spec;
        LOGGER.debug("TimeFieldSpec {} : {}", specName, timeFieldSpec);
        LOGGER.debug("Incoming TimeFieldSpec {} : {}", timeFieldSpec.getIncomingTimeColumnName(), timeFieldSpec.getIncomingGranularitySpec());
        LOGGER.debug("Outgoing TimeFieldSpec {} : {}", timeFieldSpec.getOutGoingTimeColumnName(), timeFieldSpec.getOutgoingGranularitySpec());
        specName = timeFieldSpec.getOutGoingTimeColumnName();
        dataType = timeFieldSpec.getOutgoingGranularitySpec().getDataType();
      }
      switch (dataType) {
        case BOOLEAN:
        case STRING:
          columnStatsCollectorMap.put(specName, new StringColumnPreIndexStatsCollector(spec));
          LOGGER.debug("Init StringColumnPreIndexStatsCollector for column: {}", specName);
          break;
        case INT:
          columnStatsCollectorMap.put(specName, new IntColumnPreIndexStatsCollector(spec));
          LOGGER.debug("Init IntColumnPreIndexStatsCollector for column: {}", specName);
          break;
        case LONG:
          columnStatsCollectorMap.put(specName, new LongColumnPreIndexStatsCollector(spec));
          LOGGER.debug("Init LongColumnPreIndexStatsCollector for column: {}", specName);
          break;
        case FLOAT:
          columnStatsCollectorMap.put(specName, new FloatColumnPreIndexStatsCollector(spec));
          LOGGER.debug("Init FloatColumnPreIndexStatsCollector for column: {}", specName);
          break;
        case DOUBLE:
          columnStatsCollectorMap.put(specName, new DoubleColumnPreIndexStatsCollector(spec));
          LOGGER.debug("Init DoubleColumnPreIndexStatsCollector for column: {}", specName);
          break;
        default:
          LOGGER.warn("Failed to initialize column: {}, FieldSpec: {}", specName, spec);
          break;
      }
    }
  }

  @Override
  public void build() throws Exception {
    for (final String column : columnStatsCollectorMap.keySet()) {
      columnStatsCollectorMap.get(column).seal();
    }
  }

  @Override
  public AbstractColumnStatisticsCollector getColumnProfileFor(String column) throws Exception {
    return columnStatsCollectorMap.get(column);
  }

  @Override
  public void collectRow(GenericRow row) throws Exception {
    for (final String column : row.getFieldNames()) {
      LOGGER.debug("Trying to collectRow: Column: {}, with value: {}", column, row.getValue(column));
      columnStatsCollectorMap.get(column).collect(row.getValue(column));
    }
  }

  public static <T> T convertInstanceOfObject(Object o, Class<T> clazz) {
    try {
      return clazz.cast(o);
    } catch (final ClassCastException e) {
      LOGGER.warn("Caught exception while converting instance", e);
      return null;
    }
  }

  @Override
  public void logStats() {
    try {
      for (final String column : columnStatsCollectorMap.keySet()) {
        LOGGER.info("********** logging for column : " + column + " ********************* ");
        LOGGER.info("min value : " + columnStatsCollectorMap.get(column).getMinValue());
        LOGGER.info("max value : " + columnStatsCollectorMap.get(column).getMaxValue());
        LOGGER.info("cardinality : " + columnStatsCollectorMap.get(column).getCardinality());
        LOGGER.info("length of largest column : " + columnStatsCollectorMap.get(column).getLengthOfLargestElement());
        LOGGER.info("is sorted : " + columnStatsCollectorMap.get(column).isSorted());
        LOGGER.info("column type : " + dataSchema.getFieldSpecFor(column).getDataType());
        LOGGER.info("***********************************************");
      }
    } catch (final Exception e) {
      LOGGER.error("Caught exception while logging column stats", e);
    }

  }
}
