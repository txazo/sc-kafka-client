package org.txazo.kafka.client.test.protostuff;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.Collection;
import java.util.Map;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class ProtostuffUtil {

    private static final ThreadLocal<LinkedBuffer> BUFFER_THREAD_LOCAL = ThreadLocal.withInitial(() ->
            LinkedBuffer.allocate(512));

    public static <T> byte[] serialize(T object) {
        if (isCollection(object)) {
            return serializeCollection(object);
        } else {
            return serializeObject(object);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> byte[] serializeObject(T object) {
        Schema<T> schema = RuntimeSchema.getSchema((Class<T>) object.getClass());
        LinkedBuffer buffer = BUFFER_THREAD_LOCAL.get();
        try {
            return ProtostuffIOUtil.toByteArray(object, schema, buffer);
        } finally {
            buffer.clear();
        }
    }

    public static <T> T deserializeObject(byte[] data, Class<T> classType) {
        Schema<T> schema = RuntimeSchema.getSchema(classType);
        T message = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(data, message, schema);
        return message;
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeObject(byte[] data, String className) throws ClassNotFoundException {
        return deserializeObject(data, (Class<T>) Class.forName(className));
    }

    public static <T> byte[] serializeCollection(T collection) {
        return serializeObject(ProtostuffMessage.build(collection));
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeCollection(byte[] data) {
        ProtostuffMessage message = deserializeObject(data, ProtostuffMessage.class);
        return (T) message.getData();
    }

    public static boolean isCollection(Object object) {
        return object instanceof Collection || object instanceof Map;
    }

}
