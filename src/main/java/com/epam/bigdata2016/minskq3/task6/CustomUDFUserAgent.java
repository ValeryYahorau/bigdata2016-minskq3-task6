package com.epam.bigdata2016.minskq3.task6;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;
import java.util.List;

@Description(name = "ua", value = "_FUNC_(expr) - Returns device, browser and OS from user agent.")
@UDFType(deterministic = false)
public class CustomUDFUserAgent extends GenericUDF {

    StringObjectInspector elementOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        ObjectInspector uaString = objectInspectors[0];
        this.elementOI = (StringObjectInspector) uaString;

        List structFieldNames = new ArrayList();
        List structFieldObjectInspectors = new ArrayList();

        structFieldNames.add("device");
        structFieldNames.add("browser");
        structFieldNames.add("OS");

        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        StructObjectInspector si;
        si = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);

        ListObjectInspector li;
        li = ObjectInspectorFactory.getStandardListObjectInspector(si);
        return li;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if (deferredObjects.length != 1) {
            return null;
        }
        if (deferredObjects[0].get() == null) {
            return null;
        }

        String text = elementOI.getPrimitiveJavaObject(deferredObjects[0].get());
        UserAgent ua = new UserAgent(text);

        String device = ua.getOperatingSystem() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
        String browser = ua.getBrowser() != null ? ua.getBrowser().getName() : null;
        String os = ua.getOperatingSystem() != null ? ua.getOperatingSystem().getName() : null;

        return new String[]{device, browser, os};
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}