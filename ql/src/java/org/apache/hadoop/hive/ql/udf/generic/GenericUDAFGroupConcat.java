/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.PriorityQueue;

/**
 * GenericUDAFGroupConcat
 * group_concat(col, isDistinct)
 * group_concat(col, isDistinct, separator)
 * group_concat(col, isDistinct, separator, orderBy, asc)
 *
 */
public class GenericUDAFGroupConcat extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(JsonGroupConcat.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
    return super.getEvaluator(info);
  }

  public static class GroupConcatEvaluate extends GenericUDAFEvaluator {

    ObjectInspector colOI;
    ObjectInspector isDistinctOI;
    ObjectInspector separatorOI;
    PrimitiveObjectInspector orderByOI;
    PrimitiveObjectInspector isAscOI;

    StandardStructObjectInspector internalMergeOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      if (m == Mode.PARTIAL1) {
        if (parameters.length != 2 && parameters.length != 3 && parameters.length != 5) {
          throw new HiveException("参数长度错误:" + parameters.length + " " + parameters);
        }
        colOI = parameters[0];
        isDistinctOI = parameters[1];
        if (parameters.length > 2) {
          separatorOI = parameters[2];
          if (parameters.length > 3) {
            if (parameters[3] instanceof PrimitiveObjectInspector) {
              orderByOI = (PrimitiveObjectInspector) parameters[3];
              isAscOI = (PrimitiveObjectInspector) parameters[4];
            } else {
              throw new HiveException("parameter " + parameters[3].getClass() + " must " +
                      "implement PrimitiveObjectInspector");
            }
          }
        }

      } else if (m == Mode.PARTIAL2) {

      } else if (m == Mode.COMPLETE) {

      } else if (m == Mode.FINAL) {

      }
      return null;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return null;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {

    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return null;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {

    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return null;
    }

    @AggregationType(estimable = true)
    static class GroupConcatAggBuf<T extends Comparable> extends AbstractAggregationBuffer {
      MergeQueue<T> mergeQueue = new MergeQueue<T>();
      String separator = ",";
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return model.lengthFor(mergeQueue) + model.lengthFor(separator);
      }
    }
  }

}
