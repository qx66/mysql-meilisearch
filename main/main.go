package main

import (
	"context"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/qx66/mysql-meilisearch/pkg/mysqlReplica"
)

func main() {
	config := canal.NewDefaultConfig()
	config.Addr = "127.0.0.1:3306"
	config.User = "root"
	config.Password = ""
	
	c, err := canal.NewCanal(config)
	if err != nil {
		return
	}
	
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	
	eventHandler := mysqlReplica.NewEventHandler(ctx)
	eventHandler.SetCancel(cancel)
	eventHandler.SetPosChannel(make(chan mysql.Position, 4096))
	
	go eventHandler.SavePos()
	
	c.SetEventHandler(eventHandler)
	err = c.RunFrom(mysql.Position{
		Name: "binlog.000029",
		Pos:  35992381,
	})
	
	panic(err)
}
