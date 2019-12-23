https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html


它的核心是：基于一组row（称为Frame），为表中的每个输入行计算一个返回值。每个输入行都有与其所关联的唯一Frame。


要使用窗口函数，用户需要通过下面两种方式其一来标记一个函数是按窗口函数来使用的：
1. SQL中，在所支持的函数后面添加OVER语句 ，例如：avg(revenue) OVER (...)
2. DataFrame API，在所支持的函数后面调用over方法，例如：rank().over(...)

Spark SQL执行三种类型的窗口函数：
1. 聚合函数
2. 排名函数
3. 解析函数

