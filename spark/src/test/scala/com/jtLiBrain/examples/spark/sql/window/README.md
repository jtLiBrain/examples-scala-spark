https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html


所谓窗口函数就是在一个窗口范围内使用的函数。


它的核心是：基于一组row（称为Frame），为表中的每个输入行计算一个返回值。每个输入行都有与其所关联的唯一Frame。


要使用窗口函数，用户需要通过下面两种方式其一来标记一个函数被用作窗口函数：
1. SQL中，在所支持的函数后面添加OVER语句 ，例如：avg(revenue) OVER (...)
2. DataFrame API，在所支持的函数后面调用over方法，例如：rank().over(...)


Spark SQL执行三种类型的窗口函数：
1. 聚合函数
2. 排名函数
3. 解析函数


Frame规范的三个组成元素：
1. Frame的起始边界
2. Frame的结束边界
3. Frame的类型
    3.1 ROW
    3.2 RANGE


Frame的边界类型：
1. UNBOUNDED PRECEDING:：分区中的第一行
2. UNBOUNDED FOLLOWING：分区中的最后一行
3. CURRENT ROW
4. <value> PRECEDING：
5. <value> FOLLOWING


ROW类型的Frame - 物理位置偏移
基于当前输入行在该分区所在的实际位置的位置偏移量

RANG类型的Frame - 逻辑数值偏移
基于当前输入行在该分区内、对于排序字段的数值偏移量