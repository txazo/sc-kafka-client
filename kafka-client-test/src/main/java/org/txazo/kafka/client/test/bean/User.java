package org.txazo.kafka.client.test.bean;

import io.protostuff.Tag;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.Objects;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
@Data
@NoArgsConstructor
public class User {

    @Tag(1)
    private Long id;

    @Tag(2)
    private String userName;

    @Tag(3)
    private Integer age;

    @Tag(4)
    private Date createTime;

    @Tag(5)
    private Date updateTime;

    public User(Long id, String userName, Integer age, Date createTime, Date updateTime) {
        this.id = id;
        this.userName = userName;
        this.age = age;
        this.createTime = createTime;
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        User user = (User) o;
        return Objects.equals(id, user.id) && Objects.equals(userName, user.userName) && Objects.equals(age, user.age) && Objects.equals(createTime, user.createTime) && Objects.equals(updateTime, user.updateTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, userName, age, createTime, updateTime);
    }

}
