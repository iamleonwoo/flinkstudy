package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: MarketingUserBehavior
 * Package: com.atguigu.bean
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/9 22:27
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
