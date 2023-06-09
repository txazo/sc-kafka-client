package org.txazo.kafka.client.test.protostuff;

import lombok.Data;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
@Data
public class ProtostuffMessage {

    private Object data;

    public static ProtostuffMessage build(Object data) {
        ProtostuffMessage message = new ProtostuffMessage();
        message.setData(data);
        return message;
    }

}
