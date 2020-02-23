package com.zwshao;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * create function udtf2 as 'com.zwshao.MyUDTF2';
 *
 * in data 'shao,t1-li,t2'
 *
 * select name1, name2 from input lateral view udtf2(in, '-', ',') tv as name1, name2;
 *
 *  shao  t1
 *  li    t2
 *
 */
public class MyUDTF2 extends GenericUDTF {

    List<String[]> result = new ArrayList<>();

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<ObjectInspector> ios = new ArrayList<>();
        ios.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        ios.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        List<String> names = new ArrayList<>();
        names.add("world");
        names.add("world1");
        return ObjectInspectorFactory.getStandardStructObjectInspector(names, ios);
    }

    @Override
    public void process(Object[] objects) throws HiveException {

        String content = objects[0].toString();
        String splitKey = objects[1].toString();
        String splitKey2 = objects[2].toString();

        String[] ats = content.split(splitKey);

        for (String at : ats) {
            result.clear();
            result.add(at.split(splitKey2));
            forward(result);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
