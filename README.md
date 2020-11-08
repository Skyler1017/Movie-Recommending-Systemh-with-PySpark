# 基于PySpark与协同过滤算法的电影推荐系统

[TOC]



## 协同过滤(collaborative filter)算法

- 协同过滤算法主要有两种，一种是基于用户的协同过滤算法（UserCF），另一种是基于物品的协同过滤算法（ItemCF）。
- 协同过滤算法的推荐主要有5步, 第一步为根据原理建立评分模型, 第二步为选出可作为模型中的中间标准项目, 第三步是根据公式计算中间项目的最近邻居项目集, 第四步是从最近项目集选出最优解, 第五步是推荐完成, 如下图所示:

<img src="http://skyler-pics.oss-cn-beijing.aliyuncs.com//uPic/2020/%7Bmon%7D/07/image-20201107220835202.png" alt="image-20201107220835202" style="zoom:33%;" />

### User-User CF

#### 算法思想

- 基于用户的协同过滤的基本原理相对最为朴素，其根据计算目标用户的历史偏好向量与其他用户历史偏好向量的相关性，发现目标用户偏好相似的近邻用户群，在实际应用中通常采用“K-最近邻”算法，然后基于此近邻用户群的历史偏好信息，预测目标用户对未评价项目可能产生的评分，最终得到推荐结果。基于用户的协同过滤与基于人口统计学的推荐机制都在计算用户间的相似度，但不同的是基于人口统计学的推荐机制只考虑用户本身的特征，而协同过滤则基于用户的历史偏好数据，其基本假设是喜欢类似物品的用户存在可能相同或相似的选择偏好。

#### 算法步骤

1. 找到和目标用户兴趣相似的用户集合 

2. 找到这个集合中的用户喜欢的，且目标用户没有听说过的物品推荐给目标用户。

### Item-Item CF

#### 算法思想

- 基于项目的协同过滤推荐的基本原理与基于用户的协同过滤是类似的，但其计算的是物品见的相似度。基于项目的协同过滤机制实际上是基于用户的协同过滤的一种改良，因为在大部分Web站点中，物品数量是远远小于用户数量的，而物品的数量和物品间的相似度都相对稳定。因此其计算结果拥有更长的时效性。

#### 算法步骤

1. 计算物品之间的相似度。

2. 根据物品的相似度和用户的历史行为给用户生成推荐列表。



### 常用的向量相似度度量方式

- 协同过滤算法的推荐过程分当中最重要的就是最近邻居集的计算. 一般常见的相似性度量方法有三种: 皮尔逊(Pearson)相关相似性、余弦相似性、修正的余弦相似性等, 下面依次介绍这3种度量方法.

##### 欧式距离相似度

- 欧氏距离是欧式空间中的一种距离度量方式，向量间的欧式距离是一种𝐿𝑑Ld范式，当𝑑=2d=2时，欧氏距离为：

$$
dis(x, y) = L_2 = |x-y|^2
$$



##### Person相似度

- 皮尔逊相关系数是判断两组数据与一直线拟合程度的一种度量，它在数据不是很规范(normalized)的时候会倾向于给出更好的结果，其相似度计算为：

$$
sim(x, y) = \frac{\sum_{c\in I_{xy}}(x_c-\bar{x})(y_c-\bar{x})}{\sqrt[]{\sum_{c\in I_{xy}}(x_c-\bar{x})^2}\cdot\sqrt[]{\sum_{c\in I_{xy}}(y_c-\bar{y})^2}}
$$



##### 基于余弦相似系数的相似度

- 余弦相似系数通过计算用户评分向量间的夹角的余弦值来度量向量间的相似性:

$$
sim(x,y) = cos(x, y) = \frac{x \cdot y}{|x|\cdot|y|}
$$



##### 基于修正的余弦相似系数的相似度

- 基于余弦相似系数的相似度会受到用户评分标准的影响，因此修正的余弦相似系数从用户评分向量中减去了用户对项目评分的平均值，修正后的相似度计算为：

$$
sim(x, y) = sim(x, y) = \frac{\sum_{c\in I_{xy}}(x_c-\bar{x})(y_c-\bar{x})}{\sqrt[]{\sum_{c\in I_{xy}}(x_c-\bar{x})^2}\cdot\sqrt[]{\sum_{c\in I_{xy}}(y_c-\bar{y})^2}}
$$



##### 基于Jaccard相似系数的相似度
- Jaccard相似系数用于计算两个集合间的相似性，通过计算集合的交集的模和并集的模的比值实现:

$$
sim(x,y) = \frac{x \cdot y}{|x|^2 + |y|^2 + x \cdot y}
$$



### 使用场景比较

- UserCF是推荐用户所在兴趣小组中的热点，更注重社会化，而ItemCF则是根据用户历史行为推荐相似物品，更注重个性化。UserCF一般用在新闻类网站中，而ItemCF则用在其他非新闻类网站中，如Amazon,hulu等等。因为在新闻类网站中，用户的兴趣爱好往往比较粗粒度，很少会有用户说只看某个话题的新闻，往往某个话题也不是天天会有新闻的。个性化新闻推荐更强调新闻热点，热门程度和时效性是个性化新闻推荐的重点，个性化是补充，所以UserCF给用户推荐和他有相同兴趣爱好的人关注的新闻，这样在保证了热点和时效性的同时，兼顾了个性化。另外一个原因是从技术上考虑的，作为一种物品，新闻的更新非常快，而且实时会有新的新闻出现，而如果使用ItemCF的话，需要维护一张物品之间相似度的表，实际工业界这表一般是一天一更新的，这在新闻领域是万万不能接受的。
- 在图书，电子商务和电影网站等方面，ItemCF则能更好的发挥作用。因为在这些网站中，用户的兴趣爱好一般是比较固定的，而且相比于新闻网站更细腻。在这些网站中，个性化推荐一般是给用户推荐他自己领域的相关物品。另外，这些网站的物品数量更新速度不快，一天一次更新可以接受。而且在这些网站中，用户数量往往远远大于物品数量，从存储的角度来讲，UserCF需要消耗更大的空间复杂度，另外，ItemCF可以方便的提供推荐理由，增加用户对推荐系统的信任度，所以更适合这些网站。





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

## 具体实现

### 数据读入



### 数据计算

#### 预处理-计算两个关键矩阵



#### 生成热榜数据



#### 物品相关pair的统计

#### 物品相似度的计算

#### 推荐







# reference

[大数据入门与实战-PySpark的使用教程](https://www.jianshu.com/p/5a42fe0eed4d)

[Apache Spark API doc](https://spark.apache.org/docs/latest/api/python/pyspark.html?highlight=take#pyspark-package)

[Spark quick start](https://spark.apache.org/docs/latest/quick-start.html)

