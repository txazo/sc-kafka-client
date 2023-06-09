package org.txazo.kafka.client.test.protostuff;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class CommonSerializer<T> implements Serializer<T> {

    private static final String DATA_TYPE = ":data_type";
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();

    @Override
    public byte[] serialize(String topic, T data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (data == null) {
            return null;
        } else if (data instanceof byte[]) {
            setSerializerDataType(headers, SerializerDataTypeEnum.BYTE_ARRAY);
            return (byte[]) data;
        } else if (data instanceof String) {
            setSerializerDataType(headers, SerializerDataTypeEnum.STRING);
            return STRING_SERIALIZER.serialize(topic, headers, (String) data);
        } else {
            setSerializerDataType(headers, ProtostuffUtil.isCollection(data) ?
                    SerializerDataTypeEnum.PROTOSTUFF_COLLECTION : SerializerDataTypeEnum.PROTOSTUFF_OBJECT);
            return ProtostuffUtil.serializerObject(data);
        }
    }

    private void setSerializerDataType(Headers headers, SerializerDataTypeEnum dataType) {
        headers.add(DATA_TYPE, new byte[]{dataType.getDataType()});
    }

}
