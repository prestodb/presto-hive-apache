package org.apache.hadoop.hive.serde2.lazy;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.io.Text;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory.getLazySimpleMapObjectInspector;
import static org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory.getLazyStringObjectInspector;
import static org.testng.Assert.assertEquals;

public class TestLazyMap
{
    private static final Charset US_ASCII = Charset.forName("US-ASCII");
    private static final LazyStringObjectInspector LAZY_STRING_OBJECT_INSPECTOR = getLazyStringObjectInspector(false, (byte) 0);

    @Test
    public void test()
            throws Exception
    {
        assertMapDecode("\\N\u0003ignored", ImmutableMap.of());
        assertMapDecode("\\N\u0003ignored\u0002alice\u0003apple", ImmutableMap.of(lazyString("alice"), lazyString("apple")));
        assertMapDecode("alice\u0003apple\u0002\\N\u0003ignored", ImmutableMap.of(lazyString("alice"), lazyString("apple")));
        assertMapDecode("alice\u0003apple\u0002\\N\u0003ignored\u0002bob\u0003banana",
                ImmutableMap.of(lazyString("alice"), lazyString("apple"), lazyString("bob"), lazyString("banana")));
        assertMapDecode("\\N\u0003ignored\u0002\u0003", ImmutableMap.of(lazyString(""), lazyString("")));

        HashMap<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("null", null);
        assertMapDecode("\\N\u0003ignored\u0002null\u0003\\N", expectedMap);
    }

    public static void assertMapDecode(String encodedMap, Map<? extends Object, ? extends Object> expectedMap)
    {
        LazyMap lazyMap = new LazyMap(getLazySimpleMapObjectInspector(
                LAZY_STRING_OBJECT_INSPECTOR,
                getLazyStringObjectInspector(false, (byte) 0),
                (byte) 2,
                (byte) 3,
                new Text("\\N"),
                false,
                (byte) 0
        ));

        lazyMap.init(newByteArrayRef(encodedMap), 0, encodedMap.length());

        Map<Object, Object> map = lazyMap.getMap();
        assertEquals(map, expectedMap);
    }

    private static LazyString lazyString(String string)
    {
        LazyString lazyString = new LazyString(LAZY_STRING_OBJECT_INSPECTOR);
        lazyString.init(newByteArrayRef(string), 0, string.length());
        return lazyString;
    }

    public static ByteArrayRef newByteArrayRef(String encodedMap)
    {
        ByteArrayRef bytes = new ByteArrayRef();
        bytes.setData(encodedMap.getBytes(US_ASCII));
        return bytes;
    }

}
