package com.zwshao;

import org.apache.hadoop.hive.ql.exec.UDF;

public class FirstFunc extends UDF {

    public int evaluate(int data) {
        return data + 100;
    }

    public int evaluate(int data1, int data2) {
        return data1 + data2 + 100;
    }
}
