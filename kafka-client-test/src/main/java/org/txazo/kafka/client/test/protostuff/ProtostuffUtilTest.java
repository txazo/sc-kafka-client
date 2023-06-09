package org.txazo.kafka.client.test.protostuff;

import com.google.common.collect.Lists;
import org.txazo.kafka.client.test.bean.User;
import org.txazo.kafka.client.test.common.GsonUtil;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class ProtostuffUtilTest {

    public static void main(String[] args) throws Exception {
        User user1 = new User(1001L, "root", 32, new Date(), new Date());
        User user2 = new User(1002L, "admin", 24, new Date(), new Date());
        List<User> userList = Lists.newArrayList(user1, user2);

        byte[] jsonBytes = GsonUtil.toJsonString(user1).getBytes(StandardCharsets.UTF_8.name());
        byte[] protostuffBytes = ProtostuffUtil.serialize(user1);
        User protostuffUser1 = ProtostuffUtil.deserializeObject(protostuffBytes, User.class);
        System.out.printf("%d %d %s %n", jsonBytes.length, protostuffBytes.length, Objects.equals(user1, protostuffUser1));

        jsonBytes = GsonUtil.toJsonString(userList).getBytes(StandardCharsets.UTF_8.name());
        protostuffBytes = ProtostuffUtil.serialize(userList);
        List<User> protostuffUserList = ProtostuffUtil.deserializeCollection(protostuffBytes);
        System.out.printf("%d %d %s %n", jsonBytes.length, protostuffBytes.length, Objects.equals(userList, protostuffUserList));
    }

}
