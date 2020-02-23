package com.zwshao;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * create function udtf3 as 'com.zwshao.MyUDTF3';
 *
 * in data 'zw and yp li'
 *
 * select name1, name2 from input lateral view udtf3(in, '-', ',') tv as name1, name2;
 *
 *  zw  li
 *  yp  li
 *
 */
public class MyUDTF3 extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<ObjectInspector> fieldIOs = new ArrayList<>();
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldIOs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("name");
        fieldNames.add("surname");

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldIOs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String name = objects[0].toString();

        ArrayList<Object[]> results = processInputRecord(name);

        Iterator<Object[]> it = results.iterator();

        while (it.hasNext()){
            Object[] r = it.next();
            forward(r);
        }

    }

    private ArrayList<Object[]> processInputRecord(String name) {
        ArrayList<Object[]> result = new ArrayList<Object[]>();

        // 忽略null值与空值
        if (name == null || name.isEmpty()) {
            return result;
        }

        String[] tokens = name.split("\\s+");

        if (tokens.length == 2){
            result.add(new Object[] { tokens[0], tokens[1] });
        }else if (tokens.length == 4 && tokens[1].equals("and")){
            result.add(new Object[] { tokens[0], tokens[3] });
            result.add(new Object[] { tokens[2], tokens[3] });
        }

        return result;

    }

    @Override
    public void close() throws HiveException {

    }
}
