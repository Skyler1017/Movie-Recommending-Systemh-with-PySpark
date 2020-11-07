# 基于PySpark与协同过滤算法的电影推荐系统

[TOC]



## 协同过滤(collaborative filter)算法







## [Spark](https://spark.apache.org/docs/latest/quick-start.html)

- 一个用来处理大规模数据集的分析引擎
- 提供一系列数据分析工具：Spark SQL，MLib，GraphX，Structured Streaming
- 安全性比较低

### Spark 数据结构

- Spark’s primary **abstraction** is a distributed collection of items called a Dataset
- all Datasets in Python are Dataset[Row]

### Spark 文件操作

```python
# 打开一个文件，创建一个Dataset对象
textFile = spark.read.text("README.md")

# 查看对象的行数
textFile.count()

# 查看第一行
textFile.first()

# How many lines contain "Spark"?
textFile.filter(textFile.value.contains("Spark")).count()
```

- 类似于函数式编程的思想

### Spark 编程实例

#### 查找词数最多的行

```python
from pyspark.sql.functions import *
textFile.select(size(split(textFile.value,"\s+")).name("numWords")).agg(max(col("numWords"))).collect()
```

- 可以看出 Spark 在编程难度方面有着比 MapReduce 更大的优势，极大的加快了并行算法的实现效率
- 以上代码的含义如下：
  - 首先将每一行的词数 map 到一个名为“numWords”的整形变量（形成一个DataFrame对象）
  - `agg`函数将所有行的结果聚合起来，查找结果最大的行
  - `select`函数类似于`SQL`中的含义，都是选中了一个列；`agg`类似于`SQL`中的聚合函数(count, sum, avg, max, min)等



#### WordCount

```python
wordCounts = textFile.select(explode(split(textFile.value, "\s+")).alias("word")).groupBy("word").count()
wordCounts.collect()
```

- `explode`函数起到把 `KEY, <VALUE1, VALUE2, ...>`的键值对分开的作用:

| key  | value     |
| ---- | --------- |
| a    | [1,2,3,4] |
| b    | [7,8]     |

​	会被转换成:

| key  | value |
| ---- | ----- |
| a    | 1     |
| a    | 2     |
| a    | 3     |
| a    | 4     |
| b    | 7     |
| b    | 8     |



### Spark Caching

- Spark 支持从内存中读取数据, 因此可以把热数据缓存在内存中, 加快计算效率. 想要这么做, 只需要在数据对象上调用一次``cache`函数: `linesWithSpark.cache()`



## PySpark

- 用Python写Spark代码的库

### SparkContext

- SparkContext是Spark程序的入口。当我们运行任何Spark应用程序时，会启动一个驱动程序，它具有main函数，并且此处启动了SparkContext。然后，驱动程序在工作节点上的执行程序内运行操作。
- SparkContext使用Py4J启动JVM并创建JavaSparkContext。默认情况下，PySpark将SparkContext作为'sc'提供，因此创建新的SparkContext将不起作用。



### PySpark-RDD

##### RDD

>RDD代表Resilient Distributed Dataset，它们是在**多个节点上运行和操作**以在集群上进行并行处理的元素。RDD是**不可变元素**，这意味着一旦创建了RDD，就无法对其进行更改。RDD也具有容错能力，因此在发生任何故障时，它们会自动恢复。您可以对这些RDD应用多个操作来完成某项任务。

- RDD 有两种操作方法:

  -  `Transformation`, 从一个RDD对象生成另一个新的对象, 比如`filter()`, `groupBy()`, `map`
  - `Action`, 在RDD对象上进行计算

  

##### 使用PySpark 操作 RDD 对象

- `count()`返回RDD中的元素个数

- `collect()`返回RDD中的所有元素

- `foreach(function)`仅返回满足foreach内函数条件的元素

- `filter()`返回一个包含元素的新RDD，它满足过滤器内部的功能

- `map()`应用于RDD中的每个元素来返回新的RDD, 有点像 Python 中的列表生成器, 只不过结果列表中的是key-value映射

- `reduce()`执行指定的可交换和关联二元操作后，返回RDD中的元素. (也就是说, 和Python本身的reduce函数一样, 对参数序列中元素进行累积)

- `join()`返回RDD，其中包含一对带有匹配键的元素以及该特定键的所有值, 也就是`explode()`的逆操作:

  - ```python
    from pyspark import SparkContext
    sc = SparkContext("local", "Join app")
    x = sc.parallelize([("spark", 1), ("hadoop", 4)])
    y = sc.parallelize([("spark", 2), ("hadoop", 5)])
    joined = x.join(y)
    final = joined.collect()
    print( "Join RDD -> %s" % (final))
    
    # 结果如下:
    # Join RDD -> [
    #    ('spark', (1, 2)),  
    #    ('hadoop', (4, 5))
    # ]
    ```









# reference

[大数据入门与实战-PySpark的使用教程](https://www.jianshu.com/p/5a42fe0eed4d)

[Apache Spark API doc](https://spark.apache.org/docs/latest/api/sql/index.html)

[Spark quick start](https://spark.apache.org/docs/latest/quick-start.html)

