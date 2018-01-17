# Distributed-System
mit 6.824 distributed-system project


使用golang实现分布式一致性算法Raft：选举、一致性协商、快照等功能。

基于Raft实现分布式一致性K/V存储

基于Raft实现数据中心sharding，数据分片迁移：配置文件变动，保持一致性；数据分片根据配置文件的变化，及时将数据从过期的分组迁移到新的分组，保持数据分片存储的一致性。
