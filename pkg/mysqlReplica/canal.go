package mysqlReplica

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"sync"
)

type EventHandler struct {
	sync.RWMutex
	canal.DummyEventHandler
	posCh  chan mysql.Position
	ctx    context.Context
	cancel context.CancelFunc
}

func NewEventHandler(ctx context.Context) *EventHandler {
	posCh := make(chan mysql.Position, 4096)
	
	return &EventHandler{
		ctx:   ctx,
		posCh: posCh,
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
	fmt.Printf("对表进行操作, schema: %s, table: %s, eventType: %s", schema, table, header.EventType.String())
	return nil
}

// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
// OnPosSynced 使用你自己的方法同步Position, 当force为真时, 立即同步Position
// GTID（Global Transaction ID）是MySQL中的全局事务标识符，用于唯一标识并跟踪分布式环境中的事务

func (eventHandler *EventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (eventHandler *EventHandler) OnRow(e *canal.RowsEvent) error {
	
	database := e.Table.Schema
	table := e.Table.Name
	action := e.Action
	tableColumns := e.Table.Columns // 表结构列 Table Columns
	
	if database == "temporal" {
		return nil
	}
	
	fmt.Printf("database: %s, table: %s, action: %s.\n", database, table, action)
	
	switch action {
	// delete from table; 不带 where 语句，会解析成 count(1) 条记录； count(1) = len(rows)
	case canal.DeleteAction:
		
		delData := e.Rows[0]
		sql := fmt.Sprintf("%s from %s where", canal.DeleteAction, table)
		fmt.Println("sql: ", sql)
		
		fmt.Println("delete => ", delData)
		/*
			for _, row := range e.Rows {
				for x, r := range row {
					fmt.Printf("delete => %s %s.%s %s = %v.\n", action, database, tableName, tableColumns[x].Name, r)
				}
			}
		*/
	//
	case canal.UpdateAction:
		srcData := e.Rows[0]
		newData := e.Rows[1]
		fmt.Println("srcData => ", srcData)
		fmt.Println("newData => ", newData)
		/*
			for _, row := range e.Rows {
				for x, r := range row {
					fmt.Printf("update => %s %s.%s %s = %v.\n", action, database, tableName, tableColumns[x].Name, r)
				}
			}
		*/
	
	case canal.InsertAction:
		
		newData := e.Rows[0]
		mapData := make(map[string]interface{})
		
		for x, column := range tableColumns {
			mapData[column.Name] = newData[x]
		}
		
		fmt.Println("mapData => ", mapData)
	
	default:
		fmt.Printf("Unknow Action, Database: %s, Table: %s, Action: %v\n", e.Table.Schema, e.Table.Name, action)
	}
	
	return nil
}

func (eventHandler *EventHandler) SavePos() {
	for {
		select {
		case <-eventHandler.posCh:
			
			//fmt.Println("pos: ", pos.String())
		
		case <-eventHandler.ctx.Done():
			return
		}
	}
}

func (eventHandler *EventHandler) String() string {
	return "mySQLBingLogEventHandler"
}
