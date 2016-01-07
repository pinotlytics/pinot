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
package com.linkedin.pinot.core.realtime.converter;

import java.io.File;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;

public class RealtimeSegmentConverter {

  private RealtimeSegmentImpl realtimeSegmentImpl;
  private String outputPath;
  private Schema dataSchema;
  private String tableName;
  private String segmentName;
  private String sortedColumn;

  public RealtimeSegmentConverter(RealtimeSegmentImpl realtimeSegment, String outputPath, Schema schema,
      String tableName, String segmentName, String sortedColumn) {
    realtimeSegmentImpl = realtimeSegment;
    this.outputPath = outputPath;
    if (new File(outputPath).exists()) {
      throw new IllegalAccessError("path already exists");
    }
    this.tableName = tableName;
    this.segmentName = segmentName;

    Schema newSchema = new Schema();
    for (String dimension : schema.getDimensionNames()) {
      newSchema.addSchema(dimension, schema.getFieldSpecFor(dimension));
    }
    for (String metic : schema.getMetricNames()) {
      newSchema.addSchema(metic, schema.getFieldSpecFor(metic));
    }

    TimeFieldSpec original = schema.getTimeFieldSpec();
    newSchema.addSchema(original.getName(), original);
    this.dataSchema = newSchema;
    this.sortedColumn = sortedColumn;
  }

  public void build() throws Exception {
    // lets create a record reader
    RecordReader reader;

    if (sortedColumn == null) {
      reader = new RealtimeSegmentRecordReader(realtimeSegmentImpl, dataSchema);
    } else {
      reader = new RealtimeSegmentRecordReader(realtimeSegmentImpl, dataSchema, sortedColumn);
    }
    SegmentGeneratorConfig genConfig = new SegmentGeneratorConfig(dataSchema);
    genConfig.setInputFilePath(null);

    genConfig.setTimeColumnName(dataSchema.getTimeFieldSpec().getOutGoingTimeColumnName());

    genConfig.setTimeUnitForSegment(dataSchema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType());
    genConfig.setSegmentVersion(SegmentVersion.v1);
    genConfig.setTableName(tableName);
    genConfig.setIndexOutputDir(outputPath);
    genConfig.setSegmentName(segmentName);
    final SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(genConfig, reader);
    driver.build();
  }
}
