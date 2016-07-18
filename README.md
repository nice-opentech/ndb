# ndb
基于RocksDB，100%兼容Redis的协议和命令，持久化kv存储引擎。

##架构设计
![](https://github.com/nice-opentech/ndb/blob/master/ndb.png)

**注：ndbrouter待开源**

ndbrouter 启动时，先从 ndbcenter 拉取自己的配置，并且获取所在集群的所有 ndb 节点的信息，然后把自己注册到 ndbcenter。
ndbrouter 运行时，会定期从 ndbcenter 获取当前的所有 ndb 节点，如果发生了变化会做相应的更新。
ndb 主库启动时，先从 ndbcenter 拉取自己的配置和命名空间，然后把自己注册到 ndbcenter。
ndb 主库运行时，会定期从 ndbcenter 获取当前的所有命名空间，如果发生了变化会做相应的更新。
ndb 从库启动时，先从 ndbcenter 拉取自己的配置和命名空间，并且根据节点名字找到对应的主库，然后把自己注册到 ndbcenter。
ndb 从库启动时，会定期从 ndbcenter 获取当前的所有命名空间，如果发生了变化会做相应的更新。
注意：ndb 从库的命名空间的配置不会自动从 ndbcenter 更新，而是通过 ndb 主库同步而来。
注意：如果由于某些原因导致 ndb 从库无法同步，请手动进行全量同步然后再连到主库上。
##备份恢复
通过 BACKUP dirname 命令把当前快照备份到 dirname 目录中，通过 INFO backup 命令查看备份状态。
备份完成后，可以通过 ndb --restore backup --engine.dbname nicedb 命令
把 backup 目录恢复到 nicedb 目录，然后再以 nicedb 目录启动 ndb 即可服务。
##命名空间
对于单机而言，命名空间的增删可以通过 NSNEW / NSDEL 来完成。
对于使用配置中心的集群，命名空间的增删可以通过配置中心来完成，相关的节点回自动更新（每隔一分钟更新一次）。
**命名空间一旦删除，其所有数据都会被删除，无法恢复，请谨慎操作！**

##关键实现
ndb 实现主要分这几个部分：存储引擎、网络框架、命令实现、主从同步、配置中心。
###存储引擎
存储引擎在 RocksDB 上做了简单的封装，数据使用 protobuf 格式存储。
命名空间利用 RocksDB 的 ColumnFamily 来实现，一个命名空间对应一个 ColumnFamily。
同一个 RocksDB 实例的所有 ColumnFamily 共享同一个 Write Ahead Log，
每个 ColumnFamily 有自己独立的 Memtable 和数据文件。
过期淘汰以及复杂数据类型的删除操作通过自定义的 RocksDB CompactionFilter 来实现。
###网络框架
网络框架由一个读写线程和多个处理线程组成。
读写线程负责处理所有的网络打包解包操作，处理线程负责处理具体命令的操作。
读写线程和处理线程通过一个输入和一个输出无锁队列进行通讯。
###命令实现
复杂数据类型的 KEY 的前缀编码如下：
META KEY	'\0'	version	(TYPE << 3 || SUBTYPE)
HASH: 按照 field 排序顺序存储
LIST: 按照 index 排序顺序存储
OSET: 按照 int64 排序顺序存储
ZSET: 按照 score 排序顺序存储，再按照 value 排序顺序存储
注意：ndb 可以接受浮点类型的输入，但是会把浮点类型截断成64位整数处理。
命名空间可以接受形如 namespace:{id} 或者 namespace_id(为了兼容特殊的业务) 的 KEY，
但**强烈建议**新的 KEY 统一采用 namespace:{id} 的形式。
命名空间目前支持 expire, maxlen, pruning 三种配置，配置修改后不会影响原有数据，只会影响新写入的数据。
命名空间删除后其对应的所有数据都会被删除，请谨慎操作。
命名空间新增如下命令进行管理：
NSLIST: 列出所有的命名空间
NSNEW namespace: 新建命名空间
NSDEL namespace: 删除命名空间
NSSET namespace name value: 设置命名空间的配置
NSGET namespace name: 获取命名空间的配置
###主从同步
主从同步通过从库轮询向主库拉取新数据来实现，正常情况下数据延迟在毫秒级别。
主库会对每个从库保持长链接，维护独立的上下文。
##ndbcenter
配置中心直接使用官方的 etcd 来进行数据存储，目前存储了每个集群的
ndb 的主从节点的信息，ndb 的配置信息，ndb 的命名空间及其配置，还有 ndbrouter 的配置信息。
所有配置均采用 JSON 格式存储。
