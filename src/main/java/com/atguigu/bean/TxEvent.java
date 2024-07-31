package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: TxEvent
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
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}
