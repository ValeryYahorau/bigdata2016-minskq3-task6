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

@Description(name = "uaparams", value = "_FUNC_(expr) - Returns UA type, UA family, OS name and device from user agent.")
@UDFType(deterministic = false)
public class CustomUDTFUAParams extends GenericUDTF {

    private static final String UA_TYPE = "UA_type";
    private static final String UA_FAMILY = "UA_family";
    private static final String OS_NAME = "OS_name";
    private static final String DEVICE = "Device";

    private PrimitiveObjectInspector agentDtlOI = null;
    private Object[] fwdObj = null;

    public StructObjectInspector initialize(ObjectInspector[] arg) {

        List<String> structFieldNames = new ArrayList<>();
        structFieldNames.add(UA_TYPE);
        structFieldNames.add(UA_FAMILY);
        structFieldNames.add(OS_NAME);
        structFieldNames.add(DEVICE);

        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING));

        agentDtlOI = (PrimitiveObjectInspector) arg[0];
        fwdObj = new Object[4];

        return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public void process(Object[] arg) throws HiveException {
        String uaStr = agentDtlOI.getPrimitiveJavaObject(arg[0]).toString();
        UserAgent ua = UserAgent.parseUserAgentString(uaStr);

        fwdObj[0] = ua.getBrowser() != null ? ua.getBrowser().getBrowserType().getName() : null;
        fwdObj[1] = ua.getBrowser() != null ? ua.getBrowser().getGroup().getName() : null;
        fwdObj[2] = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;
        fwdObj[3] = ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;

        this.forward(fwdObj);
    }

    @Override
    public void close() throws HiveException {
    }
}