package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: OrderEvent
 * Package: com.atguigu.bean
 * Description:
 *
 * @Author LeonWoo
 * @Create 2024/4/9 23:08
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    private Long orderId;
    private String eventType;
    //交易码
    private String txId;
    private Long eventTime;
}
