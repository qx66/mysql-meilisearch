# mysql-meilisearch

mysql-meilisearch 是一个简单的自动同步 MySQL 表数据到 meilisearch index的工具


## 关于配置

如果使用 make config 生成新的配置 struct 时，需要手动找到对应的文件中的 struct 添加如下内容

```go
type Mysql struct {
state         protoimpl.MessageState
sizeCache     protoimpl.SizeCache
unknownFields protoimpl.UnknownFields

Host                string `protobuf:"bytes,1,opt,name=host,proto3" json:"host,omitempty"`
Port                int32  `protobuf:"varint,2,opt,name=port,proto3" json:"port,omitempty"`
User                string `protobuf:"bytes,3,opt,name=user,proto3" json:"user,omitempty"`
Passwd              string `protobuf:"bytes,4,opt,name=passwd,proto3" json:"passwd,omitempty"`
BinlogCheckpointDir string `protobuf:"bytes,5,opt,name=binlogCheckpointDir,proto3" json:"binlogCheckpointDir,omitempty" yaml:"binlogCheckpointDir,omitempty"`
}

type Sync struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Db              string   `protobuf:"bytes,1,opt,name=db,proto3" json:"db,omitempty"`
	Table           string   `protobuf:"bytes,2,opt,name=table,proto3" json:"table,omitempty"`
	Index           string   `protobuf:"bytes,3,opt,name=index,proto3" json:"index,omitempty"`
	PrimaryKey      string   `protobuf:"bytes,4,opt,name=primaryKey,proto3" json:"primaryKey,omitempty" yaml:"primaryKey,omitempty"`
	FilterAbleField []string `protobuf:"bytes,5,rep,name=filterAbleField,proto3" json:"filterAbleField,omitempty" yaml:"filterAbleField,omitempty"`
}
```

PS: 目前 proto 没有新增 yaml tag 功能，这部分需要手动增加


## meilisearch

[meilisearch](https://github.com/meilisearch/meilisearch) 是一个快速搜索平台

### 快速启动

podman run -d -it --name meilisearch -p 7700:7700 -v /data/app/meilisearch/data:/meili_data -e env="production" registry.cn-hangzhou.aliyuncs.com/startops-base/meilisearch:v1.4 meilisearch --master-key="xxxxxxxxxxxx"

## 全量读取表

1. 获取 Position
2. 读取表数据，全量倒入
3. 开始监听binlog Position