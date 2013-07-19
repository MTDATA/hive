package org.apache.hadoop.hive.ql.udf.generic;

public class KeyValue<T extends Comparable> implements Comparable<KeyValue> {

    private T key;
    private Object value;

    public KeyValue() {
    }

    public KeyValue(T key, Object value) {
        this.key = key;
        this.value = value;
    }

    public T getKey() {
        return key;
    }

    public void setKey(T key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public int compareTo(KeyValue o) {
        if (o == null) {
            return 1;
        }
        if (this.key == null) {
            if (o.getKey() == null) {
                return 0;
            } else {
                return -1;
            }
        } else {
            return this.key.compareTo(o.getKey());
        }
    }
}
