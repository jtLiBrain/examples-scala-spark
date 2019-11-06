https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html


> To use window functions, users need to mark that a function is used as a window function by either
> 
> Adding an OVER clause after a supported function in SQL, e.g. avg(revenue) OVER (...); or
> Calling the over method on a supported function in the DataFrame API, e.g. rank().over(...)