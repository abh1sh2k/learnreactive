package com.abhishek.cache;

import com.abhishek.models.SegmentConfig;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Interface to be implemented by your solution
 */
public interface LookUpCache {
    public SegmentConfig[] getSegmentFor(final String orgKey, final String paramKey);
    public SegmentConfig[] getSegmentFor(final String orgKey, final String paramKey, final String paramValKey);
}
class LookupCacheImpl implements LookUpCache {
    Map<String, Map<String, Map<String, List<SegmentConfig>>>> configs =
            new HashMap<String, Map<String, Map<String, List<SegmentConfig>>>>();
    private String fileLocation = "src/test/resources/data.json";

    public SegmentConfig[] getSegmentFor(String orgKey, String paramKey) {
        return getSegmentFor(orgKey, paramKey, "");
    }

    public SegmentConfig[] getSegmentFor(String orgKey, String paramKey, String paramValKey) {
        SegmentConfig[] segmentConfigs = new SegmentConfig[0];
        if (configs.containsKey(orgKey) && configs.get(orgKey).containsKey(paramKey)
                && configs.get(orgKey).get(paramKey).containsKey(paramValKey)) {
            List<SegmentConfig> l = configs.get(orgKey).get(paramKey).get(paramValKey);
            segmentConfigs = new SegmentConfig[l.size()];
            return l.toArray(segmentConfigs);

        } else {
            return segmentConfigs;
        }
    }

    LookupCacheImpl() {
        try {
            long starttime = System.currentTimeMillis();
            readFile(fileLocation);
            System.out.println("totaltime = " + (System.currentTimeMillis() - starttime));
            System.out.println(configs.keySet());
        } catch (Exception ex) {
            System.out.printf("Exception ex ", ex);
        }
    }

    LookupCacheImpl(String location) {
        try {
            this.fileLocation = location;
            readFile(fileLocation);
            //System.out.println(configs);
        } catch (Exception ex) {
            System.out.printf("Exception ex ", ex);
        }
    }

    private void readFile(String fileLocation) throws Exception {
        JsonFactory f = new JsonFactory();
        JsonParser jp = f.createJsonParser(new File(fileLocation));
        if (jp.nextToken() == JsonToken.START_ARRAY) {
            JsonToken outer = jp.nextToken();
            while (outer != JsonToken.END_ARRAY) { // start object
                jp.nextToken();
                String organisation = jp.getCurrentName();
                Map<String, Map<String, List<SegmentConfig>>> outermostMap;
//                System.out.println("organisation = "+organisation);
                if (configs.containsKey(organisation))
                    outermostMap = configs.get(organisation);
                else
                    outermostMap = new HashMap<String, Map<String, List<SegmentConfig>>>();
                jp.nextToken();// start array
                JsonToken inner = jp.nextToken();
                Map<String, List<SegmentConfig>> innerMap;
                while (inner != JsonToken.END_ARRAY) {
                    JsonToken temp = jp.nextToken(); // field name
//                    System.out.println(jp.getCurrentLocation());
                    String paramKey = jp.getCurrentName();
//                    System.out.println("paramKey = "+paramKey);
                    if (outermostMap.containsKey(paramKey)) {
                        innerMap = outermostMap.get(paramKey);
                    } else {
                        innerMap = new HashMap<String, List<SegmentConfig>>();
                    }
                    jp.nextToken(); // start array
                    JsonToken innerMost = jp.nextToken();
                    while (innerMost != JsonToken.END_ARRAY) {
                        jp.nextToken();
                        String paramValueKeys[] = jp.getCurrentName().split("\n");
//                        System.out.println(paramValueKeys);
                        if (jp.nextToken() == JsonToken.START_OBJECT) {
                            jp.nextToken();
                            if (jp.getCurrentName() == "segmentId") {
                                String value = jp.nextTextValue();
                                List<SegmentConfig> configList;
                                for (String key : paramValueKeys) {
                                    if (innerMap.containsKey(key)) {
                                        configList = innerMap.get(key);
                                    } else
                                        configList = new ArrayList<SegmentConfig>();
                                    configList.add(new SegmentConfig(value));
                                    innerMap.put(key, configList);

                                }
                                jp.nextToken();
//                                System.out.println(jp.getCurrentLocation());
                            }
                        }
                        jp.nextToken(); // end of paramvalueKey
                        innerMost = jp.nextToken();
//                        System.out.println(jp.getCurrentLocation());
                    }
                    jp.nextToken(); //end of paramKey
                    inner = jp.nextToken();
//                    System.out.println(inner);
                    outermostMap.put(paramKey, innerMap);
                }
                jp.nextToken();
                outer = jp.nextToken();
                configs.put(organisation, outermostMap);
            }
        }
    }

    public static void main(String[] args) {
        new LookupCacheImpl();
    }
}

