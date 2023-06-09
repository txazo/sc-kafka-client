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

    public static <T> byte[] serializer(T object) {
        if (isCollection(object)) {
            return serializerCollection(object);
        } else {
            return serializerObject(object);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> byte[] serializerObject(T object) {
        Schema<T> schema = RuntimeSchema.getSchema((Class<T>) object.getClass());
        LinkedBuffer buffer = BUFFER_THREAD_LOCAL.get();
        try {
            return ProtostuffIOUtil.toByteArray(object, schema, buffer);
        } finally {
            buffer.clear();
        }
    }

    public static <T> T deserializerObject(byte[] data, Class<T> classType) {
        Schema<T> schema = RuntimeSchema.getSchema(classType);
        T message = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(data, message, schema);
        return message;
    }

    public static <T> byte[] serializerCollection(T collection) {
        ProtostuffMessage protostuffMessage = ProtostuffMessage.build(collection);
        Schema<ProtostuffMessage> schema = RuntimeSchema.getSchema(ProtostuffMessage.class);
        LinkedBuffer buffer = BUFFER_THREAD_LOCAL.get();
        try {
            return ProtostuffIOUtil.toByteArray(protostuffMessage, schema, buffer);
        } finally {
            buffer.clear();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializerCollection(byte[] data) {
        Schema<ProtostuffMessage> schema = RuntimeSchema.getSchema(ProtostuffMessage.class);
        ProtostuffMessage message = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(data, message, schema);
        return (T) message.getData();
    }

    public static boolean isCollection(Object object) {
        return object instanceof Collection || object instanceof Map;
    }

}
