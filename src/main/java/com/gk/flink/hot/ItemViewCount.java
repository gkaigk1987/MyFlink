package com.gk.flink.hot;

import lombok.Data;

/**
 * @Description: 商品点击量(窗口操作的输出类型)
 * @Author: GK
 * @Date: 2019/12/26
 */
@Data
public class ItemViewCount {

    /**
     * 商品 ID
     */
    private long itemId;

    /**
     * 商品结束时间戳
     */
    private long windowEnd;

    /**
     * 商品的点击量
     */
    private long viewCount;

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }
}
