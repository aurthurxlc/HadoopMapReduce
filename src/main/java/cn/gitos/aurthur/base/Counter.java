package cn.gitos.aurthur.base;

/**
 * Created by Aurthur on 2017/1/12.
 * 异常计数的类型
 */
enum Counter {
    TIMESKIP,        //时间格式有误
    OUTOFTIMEFLASGSKIP,    //时间超出最大时段
    LINESKIP,        //源文件行有误
    OUTOFTIMESKIP,   //不在当前时间内
    TIMEFORMATERR   //时间格式化错误
}
