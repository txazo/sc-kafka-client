package org.txazo.kafka.client.test.protostuff;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public enum SerializerDataTypeEnum {

    /**
     * 序列化数据类型
     */
    PROTOSTUFF_OBJECT((byte) 1),
    PROTOSTUFF_COLLECTION((byte) 2);

    private final byte dataType;

    SerializerDataTypeEnum(byte dataType) {
        this.dataType = dataType;
    }

    public static SerializerDataTypeEnum getByDataType(byte dataType) {
        for (SerializerDataTypeEnum dataTypeEnum : values()) {
            if (dataType == dataTypeEnum.getDataType()) {
                return dataTypeEnum;
            }
        }
        return null;
    }

    public byte getDataType() {
        return dataType;
    }

}
