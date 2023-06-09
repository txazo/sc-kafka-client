package org.txazo.kafka.client.test.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class GsonUtil {

    private static final Gson GSON = new GsonBuilder().serializeNulls().create();

    public static String toJsonString(Object object) {
        return GSON.toJson(object);
    }

}
