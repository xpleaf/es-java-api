# es-java-api
《从Lucene到Elasticsearch全文检索实战》第8章Elasticsearch Java API测试案例代码。

### 代码结构

```
├── _1client
│   └── TestClient.java
├── _2index
│   └── TestIndicesAdminClient.java
├── _3doc
│   ├── _1TestCreateDoc.java
│   ├── _2TestGet_DelDoc.java
│   ├── _3TestUpdateDoc.java
│   ├── _4TestBulkAPI.java
│   └── domain
│       └── User.java
├── _4search
│   └── EsMatchQueryTest.java
├── _5aggregation
│   ├── MaxAggregationTest.java
│   ├── _1TestIndexAgg.java
│   └── _2TestBucketAgg.java
└── _6cluster
    └── ClusterClientTest.java
```

### 与书中对应关系

> 每一个测试案例对应的内容，在代码中都有详细的注释。

```
├── _1client
│   └── TestClient.java		P242 代码清单8-1
├── _2index					P243 8.5索引管理
│   └── TestIndicesAdminClient.java
├── _3doc					P246 8.6文档管理
│   ├── _1TestCreateDoc.java	
│   ├── _2TestGet_DelDoc.java
│   ├── _3TestUpdateDoc.java
│   ├── _4TestBulkAPI.java
│   └── domain
│       └── User.java
├── _4search				P254 8.7搜索详解
│   └── EsMatchQueryTest.java
├── _5aggregation			P262 8.8聚合分析
│   ├── MaxAggregationTest.java
│   ├── _1TestIndexAgg.java
│   └── _2TestBucketAgg.java
└── _6cluster				P269 8.9集群管理
    └── ClusterClientTest.java
```

### 说明

代码大部分能够对上，因为书中给出的都是代码片段，所以我自己整理了一下，有一些是没有加进去的，主要有如下两个地方的部分代码没有加进去：

- _4search				P254 8.7搜索详解
  - 只给出了代码清单8-2，因为后面的各种查询大部分其实只需要更换一下查询条件即可，理解了代码案例，这些操作都不难；
- _5aggregation			P262 8.8聚合分析
  - 小部分聚合操作的代码是没有的；

将代码整理出来是方便大家一起学习，另外也十分感谢《从Lucene到Elasticsearch全文检索实战》的作者写的这本好书，书本的内容确实很棒！
