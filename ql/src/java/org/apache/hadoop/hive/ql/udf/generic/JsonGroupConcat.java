/*
* Copyright (c) 2010-2012 meituan.com
* All rights reserved.
*
*/
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
* group_concat
*
* @author chenchun
* @version 1.0
* @created 2013-03-20
*/
public class JsonGroupConcat extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(JsonGroupConcat.class.getName());

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        return new JsonGroupEvaluator();
    }

    public static class JsonGroupEvaluator extends GenericUDAFEvaluator {

        PrimitiveObjectInspector keyOI;
        ObjectInspector separatorOI;
        ObjectInspector valueOI;

        ListObjectInspector internalMergeOI;
        PrimitiveObjectInspector internalMergeElementOI;


        private void printOI(Mode m) {
            LOG.warn("mode="+m.name());
            System.out.println("mode="+m.name());
            System.out.println("keyOI="+keyOI);
            System.out.println("valueOI="+valueOI);
            System.out.println("separatorOI="+separatorOI);
            System.out.println("internalMergeOI="+internalMergeOI);
            System.out.println("internalMergeElementOI="+internalMergeElementOI);
        }

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL2 || m == Mode.FINAL) {
                internalMergeOI = (ListObjectInspector) parameters[0];
                internalMergeElementOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                printOI(m);
                if (m == Mode.PARTIAL2) {
                    return internalMergeOI;
                } else {
                    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                }
            } else {
                if (parameters.length != 2 && parameters.length != 3) {
                    throw new HiveException("参数长度错误:" + parameters.length + " " + parameters);
                }
                if (parameters.length == 3) {
                    if (parameters[2] instanceof PrimitiveObjectInspector) {
                        keyOI = (PrimitiveObjectInspector) parameters[2];
                    } else {
                        throw new HiveException("parameter " + parameters[2].getClass() + " must " +
                                "implement PrimitiveObjectInspector");
                    }
                } else {
                    keyOI = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
                }
                valueOI = parameters[0];
                separatorOI = parameters[1];
                if (m == Mode.PARTIAL1) {
                    internalMergeOI = ObjectInspectorFactory.getStandardListObjectInspector
                            (PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                    printOI(m);
                    return internalMergeOI;
                } else {
                    printOI(m);
                    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
                }
            }
        }

        public static class State<T extends Comparable> implements GenericUDAFEvaluator
                .AggregationBuffer {
            LinkedList<KeyValue<T>> linkedList = new LinkedList<KeyValue<T>>();
            String separator = "";

            public LinkedList<KeyValue<T>> getLinkedList() {
                return linkedList;
            }

            public void setLinkedList(LinkedList<KeyValue<T>> linkedList) {
                this.linkedList = linkedList;
            }

            public String getSeparator() {
                return separator;
            }

            public void setSeparator(String separator) {
                this.separator = separator;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new State();
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((State)agg).linkedList.clear();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            System.out.println("iterate======================================="+agg);
            if (parameters[0] != null) {
                State state = (State) agg;
                state.separator = parameters[1] == null? state.separator : (String) ObjectInspectorUtils.copyToStandardJavaObject(parameters[1], separatorOI);
                Object key = parameters.length == 3 ? (Comparable) ObjectInspectorUtils
                        .copyToStandardJavaObject(parameters[2], keyOI) : 1;
                Object value = ObjectInspectorUtils.copyToStandardJavaObject(parameters[0], valueOI);
                System.out.println("key="+key+" class="+key.getClass());
                System.out.println("value="+value+" class="+value.getClass());
                state.linkedList.add(new KeyValue((Comparable) key, value));
                System.out.println("stat=" + toJsonString(state));
                System.out.println("stat.interface="+state.getClass().getInterfaces());
                System.out.println("stat.class="+state.getClass());
                LOG.warn("stat=" + toJsonString(state));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            List<String> partialResult = new ArrayList<String>();
            partialResult.add(keyOI.getTypeName());
            partialResult.add(toJsonString(agg));
            String json = toJsonString(agg);
            System.out.println("json=" + json);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                System.out.println("partial="+partial+ " class="+partial.getClass());
                LOG.warn("partial="+partial+ " class="+partial.getClass());
                List<?> partialList = internalMergeOI.getList(partial);
                System.out.println("partialObject="+partialList+ " class="+partialList.getClass());
                System.out.println("partialList.get(0)=" + partialList.get(0) + " partialList" +
                        ".get(1)=" + partialList.get(1) +
                        " elementClass=" + partialList.get(0).getClass());
                String clazz = (String) internalMergeElementOI.getPrimitiveJavaObject(partialList
                        .get(0));
                String json = (String) internalMergeElementOI.getPrimitiveJavaObject(partialList.get(1));
                final PrimitiveObjectInspectorUtils.PrimitiveTypeEntry primitiveTypeEntry = PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(clazz);
                State partialState = null;
                if (primitiveTypeEntry == PrimitiveObjectInspectorUtils.booleanTypeEntry) {
                    partialState = (State) toObject(json, new TypeReference<State<Boolean>>() {});
                } else if (primitiveTypeEntry == PrimitiveObjectInspectorUtils.byteTypeEntry) {
                    partialState = (State) toObject(json, new TypeReference<State<Byte>>() {});
                } else if (primitiveTypeEntry == PrimitiveObjectInspectorUtils.shortTypeEntry) {
                    partialState = (State) toObject(json, new TypeReference<State<Short>>() {});
                } else if (primitiveTypeEntry == PrimitiveObjectInspectorUtils.intTypeEntry) {
                    partialState = (State) toObject(json, new TypeReference<State<Integer>>() {});
                } else if (primitiveTypeEntry == PrimitiveObjectInspectorUtils.longTypeEntry) {
                    partialState = (State) toObject(json, new TypeReference<State<Long>>() {});
                } else if (primitiveTypeEntry == PrimitiveObjectInspectorUtils.stringTypeEntry) {
                    partialState = (State) toObject(json, new TypeReference<State<String>>() {});
                } else if (primitiveTypeEntry == PrimitiveObjectInspectorUtils.floatTypeEntry) {
                    partialState = (State) toObject(json, new TypeReference<State<Float>>() {});
                } else if (primitiveTypeEntry == PrimitiveObjectInspectorUtils.doubleTypeEntry) {
                    partialState = (State) toObject(json, new TypeReference<State<Double>>() {});
                }
                if (partialState != null) {
                    State state = (State) agg;
                    state.linkedList.addAll(partialState.linkedList);
                    state.separator = partialState.separator;
                    System.out.println(toJsonString(state));
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            System.out.println("terminate==========");
            State state = (State) agg;
            System.out.println(toJsonString(state));
            LinkedList<KeyValue> list = (LinkedList<KeyValue>) state.linkedList;
            Collections.sort(list);
            String result = generateString(list, "value", state.separator);
            System.out.println("result="+result);
            LOG.warn("result="+result);
            return result;
        }
    }

    /**
     * 对象转换成json字符串
     *
     * @author zhaolei
     * @created 2011-5-9
     *
     * @param o
     *            对象
     * @return
     */
    public static String toJsonString(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @author chenchun
     * @created 2013-03-21
     *
     * @param content
     * @param valueTypeRef
     * @return
     */
    public static <E> Object toObject(String content, TypeReference<E> valueTypeRef) {
        try {
            return objectMapper.readValue(content, valueTypeRef);
        } catch (Throwable e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 把集合中某个字段变成字符串
     *
     * @author lichengwu
     * @created 2012-5-21
     *
     * @param collection
     * @param filedName
     *            集合泛型对应对象字段
     * @param separator
     *            分隔符
     * @return 字符串
     */
    public static <T> String generateString(Collection<T> collection, String filedName,
                                            String separator) {
        assert filedName != null;
        if (isBlank(separator)) {
            separator = ",";
        }
        if (collection == null) {
            return "null";
        } else if (collection.isEmpty()) {
            return "";
        }
        try {
            StringBuilder str = new StringBuilder();
            for (T obj : collection) {
                Field field = obj.getClass().getDeclaredField(filedName);
                field.setAccessible(true);
                Object object = field.get(obj);
                str.append(object.toString()).append(separator);
            }
            str.delete(str.length() - separator.length(), str.length());
            return str.toString();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 检查字符串是否有内容。
     *
     * @param obj
     * @return
     * @author lichengwu@sankuai.com
     * @created 2011-5-5
     */
    public static boolean isBlank(Object obj) {
        if (obj == null)
            return true;
        if (obj instanceof String) {
            String str = (String) obj;
            return str == null ? true : "".equals(str.trim());
        }
        try {
            String str = String.valueOf(obj);
            return str == null ? true : "".equals(str.trim());
        } catch (Exception e) {
            return true;
        }
    }


    public static void main(String[] args) {
        JsonGroupEvaluator.State state = new JsonGroupEvaluator.State();
        state.linkedList.add(new KeyValue(1, "2"));
        state.linkedList.add(new KeyValue(2, "3"));
        System.out.println(toJsonString(state));

        JsonGroupEvaluator.State json = (JsonGroupEvaluator.State) toObject(toJsonString(state),
                new TypeReference<JsonGroupEvaluator.State<Integer>>() {});
        System.out.println(json);
    }
}
