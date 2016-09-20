package com.epam.bigdata2016.minskq3.task6;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

@Description(name = "ua", value = "_FUNC_(expr) - Returns device, browser and os from user agent.")
@UDFType(deterministic = false)
public class CustomUDTFUserAgent extends GenericUDTF {

    private static final String DEVICE = "device";
    private static final String BROWSER = "browser";
    private static final String OS = "os";

    private PrimitiveObjectInspector agentDtlOI = null;
    private Object[] fwdObj = null;

    public StructObjectInspector initialize(ObjectInspector[] arg) {

        List<String> structFieldNames = new ArrayList<>();
        structFieldNames.add(DEVICE);
        structFieldNames.add(BROWSER);
        structFieldNames.add(OS);

        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));

        agentDtlOI = (PrimitiveObjectInspector) arg[0];
        fwdObj = new Object[3];

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public void process(Object[] arg) throws HiveException {
        String uaStr = agentDtlOI.getPrimitiveJavaObject(arg[0]).toString();
        UserAgent ua = UserAgent.parseUserAgentString(uaStr);

        String device = ua.getOperatingSystem() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
        String browser = ua.getBrowser() != null ? ua.getBrowser().getName() : null;
        String os = ua.getOperatingSystem() != null ? ua.getOperatingSystem().getName() : null;

        fwdObj[0] = device;
        fwdObj[1] = browser;
        fwdObj[2] = os;

        this.forward(fwdObj);
    }

    @Override
    public void close() throws HiveException {
        forward(fwdObj);
    }
}