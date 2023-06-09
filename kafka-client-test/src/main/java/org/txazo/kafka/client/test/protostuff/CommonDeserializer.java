package org.txazo.kafka.client.test.protostuff;

import com.google.common.collect.Lists;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class CommonDeserializer implements Deserializer<Object> {

    private static final String DATA_TYPE = ":data_type";
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

    @Override
    public Object deserialize(String topic, byte[] data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        SerializerDataTypeEnum dataTypeEnum = getSerializerDataType(headers);
        if (dataTypeEnum == null) {
            return STRING_DESERIALIZER.deserialize(topic, headers, data);
        }
        switch (dataTypeEnum) {
            case STRING:
                return STRING_DESERIALIZER.deserialize(topic, headers, data);
            case PROTOSTUFF_COLLECTION:
                return ProtostuffUtil.deserializerCollection(data);
            case BYTE_ARRAY:
            case PROTOSTUFF_OBJECT:
                return data;
            default:
                throw new UnsupportedOperationException("Unsupported serializer data type");
        }
    }

    private SerializerDataTypeEnum getSerializerDataType(Headers headers) {
        for (Header header : Lists.newArrayList(headers.headers(DATA_TYPE).iterator())) {
            byte[] value = header.value();
            if (value != null && value.length == 1) {
                return SerializerDataTypeEnum.getByDataType(value[0]);
            }
        }
        return null;
    }

}
