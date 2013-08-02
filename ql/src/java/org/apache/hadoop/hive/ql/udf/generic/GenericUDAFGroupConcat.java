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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * GenericUDAFGroupConcat
 * group_concat(col, isDistinct)
 * group_concat(col, isDistinct, separator)
 * group_concat(col, isDistinct, separator, orderBy, asc)
 *
 */
public class GenericUDAFGroupConcat extends AbstractGenericUDAFResolver {

  static final Log LOG = LogFactory.getLog(GenericUDAFGroupConcat.class.getName());

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
    return super.getEvaluator(info);
  }

  public static class GroupConcatEvaluate extends GenericUDAFEvaluator {

    ObjectInspector colOI;
    ObjectInspector separatorOI;
    ObjectInspector isDistinctOI;
    ObjectInspector isAscOI;
    PrimitiveObjectInspector orderByOI;
    StandardListObjectInspector heapOI;
    StandardStructObjectInspector keyValueStructOI;
    StandardStructObjectInspector internalMergeOI;

    StructField colField;
    StructField orderByField;
    StructField isDistinctField;
    StructField separatorField;
    StructField isAscField;
    StructField heapField;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        if (parameters.length != 2 && parameters.length != 3 && parameters.length != 5) {
          throw new HiveException("参数长度错误:" + parameters.length + " " + parameters);
        }
        //group_concat(col, isDistinct)
        colOI = parameters[0];
        isDistinctOI = parameters[1];
        if (parameters.length > 2) {
          //group_concat(col, isDistinct, separator)
          separatorOI = parameters[2];
          if (parameters.length > 3) {
            //group_concat(col, isDistinct, separator, orderBy, asc)
            if (parameters[3] instanceof PrimitiveObjectInspector) {
              orderByOI = (PrimitiveObjectInspector) parameters[3];
              isAscOI = parameters[4];
            } else {
              throw new HiveException("parameter " + parameters[3].getClass() + " must " +
                      "implement PrimitiveObjectInspector");
            }
          }
        }
        if (m == Mode.PARTIAL1) {
          terminatePartialOI();
          return internalMergeOI;
        } else {
          return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }
      } else {
        internalMergeOI = (StandardStructObjectInspector) parameters[0];
        heapField = internalMergeOI.getStructFieldRef("heap");
        heapOI = (StandardListObjectInspector) heapField.getFieldObjectInspector();
        keyValueStructOI = (StandardStructObjectInspector) heapOI.getListElementObjectInspector();
        colField = keyValueStructOI.getStructFieldRef("key");
        colOI = colField.getFieldObjectInspector();
        orderByField = keyValueStructOI.getStructFieldRef("value");
        orderByOI = (PrimitiveObjectInspector) orderByField.getFieldObjectInspector();
        isDistinctField = internalMergeOI.getStructFieldRef("isDistinct");
        isDistinctOI = isDistinctField.getFieldObjectInspector();
        separatorField = internalMergeOI.getStructFieldRef("separator");
        separatorOI = separatorField.getFieldObjectInspector();
        isAscField = internalMergeOI.getStructFieldRef("isAsc");
        isAscOI = isAscField.getFieldObjectInspector();
        if (m == Mode.PARTIAL2) {
          return internalMergeOI;
        } else {
          return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }
      }
    }

    private void terminatePartialOI() {
      List<String> fieldNames = new ArrayList<String>();
      fieldNames.add("heap");
      fieldNames.add("separator");
      fieldNames.add("isDistinct");
      fieldNames.add("isAsc");
      List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
      List<String> keyValueNames = new ArrayList<String>();
      keyValueNames.add("key");
      keyValueNames.add("value");
      List<ObjectInspector> keyValueOI = new ArrayList<ObjectInspector>();
      keyValueOI.add(colOI);
      keyValueOI.add(orderByOI == null ? PrimitiveObjectInspectorFactory.writableIntObjectInspector : orderByOI);
      keyValueStructOI = ObjectInspectorFactory.getStandardStructObjectInspector(keyValueNames, keyValueOI);
      heapOI = ObjectInspectorFactory.getStandardListObjectInspector(keyValueStructOI);
      fieldInspectors.add(heapOI);
      fieldInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      fieldInspectors.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
      fieldInspectors.add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
      internalMergeOI = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new GroupConcatAggBuf();
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((GroupConcatAggBuf) agg).reset();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      if (parameters[0] != null) {
        GroupConcatAggBuf aggBuf = (GroupConcatAggBuf) agg;
        Object value = ObjectInspectorUtils.copyToStandardObject(parameters[0], colOI);
        Object key = orderByOI == null? 1 : ObjectInspectorUtils.copyToStandardObject(parameters[3], orderByOI);
        aggBuf.mergeQueue.insert(new KeyValue((Comparable) key, value));
        if (separatorOI != null) {
          aggBuf.separator = PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(parameters[2]);
        }
        if (isDistinctOI != null) {
          aggBuf.isDistinct = (Boolean) PrimitiveObjectInspectorFactory.javaBooleanObjectInspector.getPrimitiveJavaObject(parameters[1]);
        }
        if (isAscOI != null) {
          aggBuf.isAsc = (Boolean) PrimitiveObjectInspectorFactory.javaBooleanObjectInspector.getPrimitiveJavaObject(parameters[4]);
        }
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      GroupConcatAggBuf aggBuf = (GroupConcatAggBuf) agg;
      return new Object[] {aggBuf.mergeQueue.getHeap(), aggBuf.separator, aggBuf.isDistinct, aggBuf.isAsc};
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      if (partial != null) {
        GroupConcatAggBuf aggBuf = (GroupConcatAggBuf) agg;
        aggBuf.isDistinct = (Boolean) internalMergeOI.getStructFieldData(partial, isDistinctField);
        aggBuf.isAsc = (Boolean) internalMergeOI.getStructFieldData(partial, isAscField);
        Object heapObj = internalMergeOI.getStructFieldData(partial, heapField);
        List<KeyValue<?>> heap = (List<KeyValue<?>>) heapOI.getList(heapObj);
        if (heap != null) {
          for (KeyValue<?> keyValue : heap) {
            aggBuf.mergeQueue.insert(keyValue);
          }
        }
        aggBuf.separator = (String) internalMergeOI.getStructFieldData(partial, separatorField);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      GroupConcatAggBuf aggBuf = (GroupConcatAggBuf) agg;
      StringBuilder str = new StringBuilder();
      KeyValue[] heap = (KeyValue[]) aggBuf.mergeQueue.getHeap();
      if (heap != null) {
        for (KeyValue<?> keyValue : heap) {
          str.append(keyValue.getValue());
          str.append(aggBuf.separator);
        }
        str.delete(str.length() - aggBuf.separator.length(), str.length());
      }
      return str.toString();
    }

    @AggregationType(estimable = true)
    static class GroupConcatAggBuf<T extends Comparable> extends AbstractAggregationBuffer {
      MergeQueue<KeyValue<T>> mergeQueue = new MergeQueue<KeyValue<T>>();
      String separator = ",";
      boolean isDistinct = false;
      boolean isAsc = true;
      @Override
      public int estimate() {
        JavaDataModel model = JavaDataModel.get();
        return model.lengthFor(mergeQueue) + model.lengthFor(separator) + JavaDataModel.PRIMITIVES1 * 2;
      }

      public void reset() {
        mergeQueue.clear();
        separator = ",";
        isDistinct = false;
        isAsc = true;
      }
    }
  }

}
