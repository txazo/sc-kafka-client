package org.txazo.kafka.client.test.protostuff;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class CommonSerializer<T> implements Serializer<T> {

    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();

    @Override
    public byte[] serialize(String topic, T data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (data == null) {
            return null;
        } else if (data instanceof String) {
            return STRING_SERIALIZER.serialize(topic, headers, (String) data);
        } else {
            setSerializeDataType(headers, ProtostuffUtil.isCollection(data) ?
                    SerializerDataTypeEnum.PROTOSTUFF_COLLECTION : SerializerDataTypeEnum.PROTOSTUFF_OBJECT);
            if (!ProtostuffUtil.isCollection(data)) {
                headers.add(ProtostuffConstant.HEADER_CLASS_NAME_KEY, STRING_SERIALIZER.serialize(null, data.getClass().getName()));
            }
            return ProtostuffUtil.serializer(data);
        }
    }

    private void setSerializeDataType(Headers headers, SerializerDataTypeEnum dataType) {
        headers.add(ProtostuffConstant.HEADER_DATA_TYPE_KEY, new byte[]{dataType.getDataType()});
    }

}
