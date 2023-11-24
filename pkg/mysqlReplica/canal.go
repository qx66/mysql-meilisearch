package mysqlReplica

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/qx66/mysql-meilisearch/internal/conf"
	"github.com/qx66/mysql-meilisearch/pkg/meilisearch"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"sync"
)

type EventHandler struct {
	sync.RWMutex
	canal.DummyEventHandler
	meiliSearchClient *meilisearch.Client
	posCh             chan mysql.Position
	ctx               context.Context
	cancel            context.CancelFunc
	dataDir           string
	logger            *zap.Logger
	sync              []*conf.Sync
}

func NewEventHandler(ctx context.Context, meiliSearchClient *meilisearch.Client, sync []*conf.Sync, dataDir string, logger *zap.Logger) *EventHandler {
	posCh := make(chan mysql.Position, 4096)
	
	return &EventHandler{
		ctx:               ctx,
		meiliSearchClient: meiliSearchClient,
		sync:              sync,
		dataDir:           dataDir,
		logger:            logger,
		posCh:             posCh,
	}
}

func (eventHandler *EventHandler) SetCancel(cancel context.CancelFunc) {
	eventHandler.cancel = cancel
}

func (eventHandler *EventHandler) SetPosChannel(posCh chan mysql.Position) {
	eventHandler.posCh = posCh
}

// 当binlog日志轮转时

func (eventHandler *EventHandler) OnRotate(header *replication.EventHeader, event *replication.RotateEvent) error {
	eventHandler.posCh <- mysql.Position{
		Name: string(event.NextLogName),
		Pos:  uint32(event.Position),
	}
	return nil
}

// 当执行 DDL 语句时 (⚠️注意: OnTableChanged 在其之前执行)

func (eventHandler *EventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, q *replication.QueryEvent) error {
	eventHandler.posCh <- nextPos
	return nil
}

// XID 代表"Transaction ID"，它记录了事务的唯一标识符。每个事务都会被分配一个唯一的XID，用于标识该事务在数据库中的执行过程。
// XID 对于MySQL的数据恢复、主从复制、备份和数据库故障恢复非常重要，因为它允许系统识别和跟踪事务，确保在不同MySQL实例之间正确地应用相同的数据更改。
// XID 也在数据库管理工具和监控工具中用于识别和跟踪事务的执行。

func (eventHandler *EventHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	eventHandler.posCh <- nextPos
	return nil
}

// OnTableChanged is called when the table is created, altered, renamed or dropped.
// You need to clear the associated data like cache with the table.
// It will be called before OnDDL.
// 不包含 database 相关操作 (create database / drop database 等)
// 包含(create table / drop table / truncate table / alter table / rename table)

func (eventHandler *EventHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	eventHandler.logger.Warn(
		"OnTableChanged",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.String("eventType", header.EventType.String()),
	)
	return nil
}

// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
// OnPosSynced 使用你自己的方法同步Position, 当force为真时, 立即同步Position
// GTID（Global Transaction ID）是MySQL中的全局事务标识符，用于唯一标识并跟踪分布式环境中的事务

func (eventHandler *EventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

// 当表内容发生变化时, e.Table.Columns 以最新的表内容为准

func (eventHandler *EventHandler) OnRow(e *canal.RowsEvent) error {
	
	database := e.Table.Schema
	table := e.Table.Name
	action := e.Action
	tableColumns := e.Table.Columns // 表结构列 Table Columns
	
	var hasHit bool = false
	var index string
	var primaryKey string
	
	for _, s := range eventHandler.sync {
		
		if database == s.Db && table == s.Table {
			index = s.Index
			hasHit = true
			primaryKey = s.PrimaryKey
		}
	}
	
	if !hasHit {
		return nil
	}
	
	switch action {
	// delete from table; 不带 where 语句，会解析成 count(1) 条记录； count(1) = len(rows)
	case canal.DeleteAction:
		
		delData := e.Rows[0]
		eventHandler.logger.Info(
			"DeleteAction",
			zap.String("database", database),
			zap.String("table", table),
			zap.String("action", action),
			zap.Any("delData", delData),
		)
		/*
			for _, row := range e.Rows {
				for x, r := range row {
					fmt.Printf("delete => %s %s.%s %s = %v.\n", action, database, tableName, tableColumns[x].Name, r)
				}
			}
		*/
		
		var identifier string
		for x, columns := range tableColumns {
			if columns.Name == primaryKey {
				if v, ok := delData[x].(string); ok {
					identifier = v
				}
			}
		}
		
		if identifier == "" {
			return errors.New("DeleteAction 未匹配 identifier 值")
		}
		
		return eventHandler.meiliSearchClient.DeleteDocument(index, identifier)
	//
	case canal.UpdateAction:
		srcData := e.Rows[0]
		newData := e.Rows[1]
		
		eventHandler.logger.Info(
			"UpdateAction",
			zap.String("database", database),
			zap.String("table", table),
			zap.String("action", action),
			
			zap.Any("srcData", srcData),
			zap.Any("newData", newData),
		)
		/*
			for _, row := range e.Rows {
				for x, r := range row {
					fmt.Printf("update => %s %s.%s %s = %v.\n", action, database, tableName, tableColumns[x].Name, r)
				}
			}
		*/
		
		docs := make(map[string]interface{})
		for x, columns := range tableColumns {
			name := columns.Name
			docs[name] = newData[x]
		}
		
		return eventHandler.meiliSearchClient.UpdateDocuments(index, primaryKey, docs)
	
	case canal.InsertAction:
		
		newData := e.Rows[0]
		//mapData := make(map[string]interface{})
		
		eventHandler.logger.Info(
			"InsertAction",
			zap.String("database", database),
			zap.String("table", table),
			zap.String("action", action),
			
			zap.Any("tableColumns", tableColumns),
			zap.Any("newData", newData),
		)
		
		// 长度不一致，可能因为表结构已经发生变化
		if len(newData) != len(tableColumns) {
			return errors.New("表结构可能发生变化")
		}
		
		docs := make(map[string]interface{})
		for x, columns := range tableColumns {
			name := columns.Name
			columnData := newData[x]
			
			switch v := columnData.(type) {
			case string:
				docs[name] = v
			case int:
				docs[name] = v
			case []byte:
				docs[name] = string(v)
			default:
				docs[name] = v
			}
		}
		
		err := eventHandler.meiliSearchClient.CreateDocs(index, docs, primaryKey)
		return err
	
	default:
		eventHandler.logger.Error(
			"UnKnow Action",
			zap.String("database", database),
			zap.String("table", table),
			zap.String("action", action),
		)
	}
	
	return nil
}

// 每次执行前进行更新

func (eventHandler *EventHandler) UpdateAttributes() error {
	for _, s := range eventHandler.sync {
		primaryKey := s.PrimaryKey
		filterAbleField := s.FilterAbleField
		
		var filterAbleFieldHasPrimaryKey bool = false
		for _, f := range filterAbleField {
			if f == primaryKey {
				filterAbleFieldHasPrimaryKey = true
			}
		}
		
		if !filterAbleFieldHasPrimaryKey {
			filterAbleField = append(filterAbleField, primaryKey)
		}
		
		//
		err := eventHandler.meiliSearchClient.CreateIndex(s.Index, primaryKey)
		if err != nil {
			return err
		}
		
		//
		err = eventHandler.meiliSearchClient.UpdateAttributes(s.Index, filterAbleField...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (eventHandler *EventHandler) SavePos() {
	for {
		select {
		case position := <-eventHandler.posCh:
			eventHandler.savePos(position)
		case <-eventHandler.ctx.Done():
			return
		}
	}
}

func (eventHandler *EventHandler) savePos(position mysql.Position) {
	eventHandler.Lock()
	defer eventHandler.Unlock()
	
	binlogPosCheckpointFile := filepath.Join(eventHandler.dataDir, "checkpoint")
	f, err := os.OpenFile(binlogPosCheckpointFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0640)
	defer f.Close()
	
	if err != nil {
		eventHandler.logger.Error(
			"打开文件checkpoint文件失败",
			zap.String("checkpointFilePath", binlogPosCheckpointFile),
			zap.Error(err),
		)
		return
	}
	
	s := fmt.Sprintf("%s %d", position.Name, position.Pos)
	_, err = f.Write([]byte(s))
	if err != nil {
		eventHandler.logger.Error(
			"写入checkpoint文件失败",
			zap.String("checkpointFilePath", binlogPosCheckpointFile),
			zap.Error(err),
		)
		return
	}
	
	eventHandler.logger.Debug(
		"写入checkpoint文件成功",
		zap.String("checkpointFilePath", binlogPosCheckpointFile),
	)
	return
}

// 第一次执行，初始化全量数据到 MeiliSearch

func (eventHandler *EventHandler) FirstInitTable(c *canal.Canal) error {
	
	for _, s := range eventHandler.sync {
		// 获取 table 信息
		table, err := c.GetTable(s.Db, s.Table)
		
		index := s.Index
		primaryKey := s.PrimaryKey
		
		if err != nil {
			eventHandler.logger.Error(
				"初始化数据库表失败, 获取表信息失败",
				zap.String("database", s.Db),
				zap.String("table", s.Table),
				zap.Error(err),
			)
			return err
		}
		
		// 获取 table 数据
		var offset int64 = 0
		for {
			sql := fmt.Sprintf("select * from %s.%s limit %d, 1000;", s.Db, s.Table, offset)
			r, err := c.Execute(sql)
			if err != nil {
				eventHandler.logger.Error(
					"初始化数据库表失败,执行SQL失败",
					zap.String("database", s.Db),
					zap.String("table", s.Table),
					zap.String("sql", sql),
					zap.Error(err),
				)
				return err
			}
			
			if len(r.Values) == 0 {
				break
			}
			
			var docs []map[string]interface{}
			
			for _, v := range r.Values {
				doc := make(map[string]interface{})
				
				for n, x := range v {
					switch rv := x.Value().(type) {
					case string:
						doc[table.Columns[n].Name] = rv
					case int:
						doc[table.Columns[n].Name] = rv
					case []byte:
						doc[table.Columns[n].Name] = string(rv)
					default:
						doc[table.Columns[n].Name] = rv
					}
				}
				
				docs = append(docs, doc)
			}
			
			err = eventHandler.meiliSearchClient.CreateDocs(index, docs, primaryKey)
			if err != nil {
				eventHandler.logger.Error(
					"初始化数据库表失败, 插入数据到 MeiliSearch失败",
					zap.String("database", s.Db),
					zap.String("table", s.Table),
					zap.String("sql", sql),
					zap.Error(err),
				)
				return err
			}
			
			offset += 1000
		}
		
		eventHandler.logger.Info(
			"初始化数据库表成功",
			zap.String("database", s.Db),
			zap.String("table", s.Table),
		)
	}
	
	return nil
}

func (eventHandler *EventHandler) String() string {
	return "mySQLBingLogEventHandler"
}
