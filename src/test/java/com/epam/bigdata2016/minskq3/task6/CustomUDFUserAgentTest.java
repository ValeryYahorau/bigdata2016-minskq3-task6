package com.epam.bigdata2016.minskq3.task6;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CustomUDFUserAgentTest {

    @Test
    public void testComplexUDFReturnsCorrectValues() throws HiveException {

        String userAgent = "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)";
        ObjectInspector inputOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);

        CustomUDFUserAgent udf = new CustomUDFUserAgent();
        udf.initialize(new ObjectInspector[]{inputOI});
        Object result = udf.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(new Text(userAgent))});

        assertThat(result, instanceOf(String[].class));
        String[] parsedUa = (String[]) result;
        assertThat(parsedUa[0], is("Computer"));
        assertThat(parsedUa[1], is("Internet Explorer 9"));
        assertThat(parsedUa[2], is("Windows 7"));
    }
}