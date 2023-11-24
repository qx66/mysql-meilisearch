package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/qx66/mysql-meilisearch/internal/conf"
	"github.com/qx66/mysql-meilisearch/pkg/meilisearch"
	"github.com/qx66/mysql-meilisearch/pkg/mysqlReplica"
	"github.com/startopsz/rule/pkg/os/filesystem"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var configPath string

func init() {
	flag.StringVar(&configPath, "configPath", "", "-configPath")
}

func main() {
	flag.Parse()
	logger, err := zap.NewProduction()
	if err != nil {
		panic("创建 zap logger 失败")
	}
	
	//
	f, err := os.Open(configPath)
	defer f.Close()
	if err != nil {
		logger.Error(
			"加载配置文件失败",
			zap.String("configPath", configPath),
			zap.Error(err),
		)
		return
	}
	
	//
	var buf bytes.Buffer
	_, err = io.Copy(&buf, f)
	if err != nil {
		logger.Error(
			"加载配置文件copy内容失败",
			zap.Error(err),
		)
		return
	}
	
	//
	var bootstrap conf.Bootstrap
	err = yaml.Unmarshal(buf.Bytes(), &bootstrap)
	if err != nil {
		logger.Error(
			"序列化配置失败",
			zap.Error(err),
		)
		return
	}
	
	//
	config := canal.NewDefaultConfig()
	config.Addr = fmt.Sprintf("%s:%d", bootstrap.Mysql.Host, bootstrap.Mysql.Port)
	config.User = bootstrap.Mysql.User
	config.Password = bootstrap.Mysql.Passwd
	
	c, err := canal.NewCanal(config)
	if err != nil {
		logger.Error(
			"NewCanal失败",
			zap.Error(err),
		)
		return
	}
	
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	
	//
	if !filesystem.Exists(bootstrap.Mysql.BinlogCheckpointDir) {
		err = os.Mkdir(bootstrap.Mysql.BinlogCheckpointDir, 0750)
		if err != nil {
			logger.Error(
				"创建 binlog checkpoint 目录失败",
				zap.Error(err),
			)
			return
		}
	}
	
	//
	meiliSearchClient := meilisearch.NewClient(bootstrap.Meilisearch.Host, bootstrap.Meilisearch.Apikey, logger)
	eventHandler := mysqlReplica.NewEventHandler(ctx, meiliSearchClient, bootstrap.Sync, bootstrap.Mysql.BinlogCheckpointDir, logger)
	
	// Meilisearch - 初始化 & 校验 Meilisearch 信息
	err = eventHandler.UpdateAttributes()
	if err != nil {
		logger.Error(
			"初始化 Meilisearch 索引信息失败",
			zap.String("Meilisearch Host", bootstrap.Meilisearch.Host),
			zap.Error(err),
		)
		return
	}
	
	eventHandler.SetCancel(cancel)
	eventHandler.SetPosChannel(make(chan mysql.Position, 4096))
	
	//c.Execute()
	
	go eventHandler.SavePos()
	
	c.SetEventHandler(eventHandler)
	
	//
	binlogPosCheckpointFile := filepath.Join(bootstrap.Mysql.BinlogCheckpointDir, "checkpoint")
	var startPosition mysql.Position
	
	if filesystem.Exists(binlogPosCheckpointFile) {
		f, err := os.Open(binlogPosCheckpointFile)
		if err != nil {
			logger.Error(
				"打开 MySQL Binlog Pos CheckPoint 文件失败",
				zap.Error(err),
			)
			return
		}
		checkpointByte, err := io.ReadAll(f)
		if err != nil {
			logger.Error(
				"读取 MySQL Binlog Pos CheckPoint 文件内容失败",
				zap.Error(err),
			)
			return
		}
		
		checkpoints := strings.Split(string(checkpointByte), " ")
		if len(checkpoints) != 2 {
			logger.Error(
				"MySQL Binlog Pos CheckPoint 文件格式不符合规范",
				zap.Error(err),
			)
			return
		}
		
		posNumber, err := strconv.ParseUint(checkpoints[1], 10, 64)
		if err != nil {
			logger.Error(
				"MySQL Binlog Pos CheckPoint PosNumber异常",
				zap.Error(err),
			)
			return
		}
		
		startPosition.Name = checkpoints[0]
		startPosition.Pos = uint32(posNumber)
		
		logger.Info(
			"从 checkpoint 文件中读取 Position",
			zap.String("Position", startPosition.String()),
		)
	} else {
		pos, err := c.GetMasterPos()
		if err != nil {
			logger.Error(
				"获取Master Pos失败",
				zap.Error(err),
			)
			return
		}
		
		startPosition = pos
		
		logger.Info(
			"通过GetMasterPos获取Position",
			zap.String("Position", startPosition.String()),
		)
		
		err = eventHandler.FirstInitTable(c)
		if err != nil {
			logger.Error(
				"第一次运行，全量表数据同步失败",
				zap.Error(err),
			)
			return
		}
	}
	
	//
	err = c.RunFrom(startPosition)
	
	if err != nil {
		logger.Error(
			"启动程序失败",
			zap.Error(err),
		)
	}
	
}
