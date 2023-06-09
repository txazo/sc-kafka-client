package org.txazo.kafka.client.test.protostuff;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class CommonDeserializer implements Deserializer<Object> {

    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

    @Override
    public Object deserialize(String topic, byte[] data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        SerializerDataTypeEnum dataTypeEnum = getSerializeDataType(headers);
        if (dataTypeEnum == null) {
            return STRING_DESERIALIZER.deserialize(topic, headers, data);
        }
        switch (dataTypeEnum) {
            case PROTOSTUFF_COLLECTION:
                return ProtostuffUtil.deserializeCollection(data);
            case PROTOSTUFF_OBJECT: {
                String className = getSerializeClassName(headers);
                if (StringUtils.isBlank(className)) {
                    throw new IllegalStateException("Header className not exists");
                }
                try {
                    return ProtostuffUtil.deserializeObject(data, className);
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("Class " + className + " not found");
                }
            }
            default:
                throw new UnsupportedOperationException("Unsupported serialize data type");
        }
    }

    private SerializerDataTypeEnum getSerializeDataType(Headers headers) {
        for (Header header : Lists.newArrayList(headers.headers(ProtostuffConstant.HEADER_DATA_TYPE_KEY).iterator())) {
            byte[] value = header.value();
            if (value != null && value.length == 1) {
                return SerializerDataTypeEnum.getByDataType(value[0]);
            }
        }
        return null;
    }

    private String getSerializeClassName(Headers headers) {
        for (Header header : Lists.newArrayList(headers.headers(ProtostuffConstant.HEADER_CLASS_NAME_KEY).iterator())) {
            return header.value() != null ? STRING_DESERIALIZER.deserialize(null, header.value()) : null;
        }
        return null;
    }

}
