package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: UserBehavior
 * Package: com.atguigu.bean
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/9 21:01
 * @Version 1.0
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;
}
