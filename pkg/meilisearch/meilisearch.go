package meilisearch

import (
	"fmt"
	"github.com/meilisearch/meilisearch-go"
	"go.uber.org/zap"
)

type Client struct {
	client *meilisearch.Client
	logger *zap.Logger
}

func NewClient(host, apiKey string, logger *zap.Logger) *Client {
	client := meilisearch.NewClient(
		meilisearch.ClientConfig{
			Host:   host,
			APIKey: apiKey,
		},
	)
	
	return &Client{
		client: client,
		logger: logger,
	}
}

func (client *Client) UpdateAttributes(indexName string, filterAbleField ...string) error {
	
	index := client.client.Index(indexName)
	task, err := index.UpdateFilterableAttributes(&filterAbleField)
	
	if err != nil {
		client.logger.Error("更新meilisearch index属性失败",
			zap.String("error", err.Error()),
			zap.String("status", string(task.Status)),
			zap.String("indexUid", task.IndexUID),
			zap.String("type", string(task.Type)),
			zap.Int64("taskUid", task.TaskUID),
			zap.Int64("enqueuedAt", task.EnqueuedAt.Unix()),
		)
	} else {
		client.logger.Info("更新meilisearch index属性成功",
			zap.String("error", err.Error()),
			zap.String("status", string(task.Status)),
			zap.String("indexUid", task.IndexUID),
			zap.String("type", string(task.Type)),
			zap.Int64("taskUid", task.TaskUID),
			zap.Int64("enqueuedAt", task.EnqueuedAt.Unix()),
		)
	}
	return err
}

func (client *Client) Search(indexName string, keyWord string, page int64) {
	index := client.client.Index(indexName)
	
	res, err := index.Search(keyWord, &meilisearch.SearchRequest{
		Limit: 10,
		Page:  page,
	})
	
	if err != nil {
		fmt.Println("err: ", err.Error())
		return
	}
	
	for _, hit := range res.Hits {
		fmt.Println("hit: ", hit)
		fmt.Println("")
	}
}

func (client *Client) CreateDocs(indexName string, docs []map[string]interface{}) error {
	
	index := client.client.Index(indexName)
	
	task, err := index.AddDocuments(docs)
	if err != nil {
		client.logger.Error("添加文档失败",
			zap.String("error", err.Error()),
		)
		return err
	}
	
	client.logger.Error("调用添加文档成功",
		zap.String("status", string(task.Status)),
		zap.String("indexUid", task.IndexUID),
		zap.String("type", string(task.Type)),
		zap.Int64("taskUid", task.TaskUID),
		zap.Int64("enqueuedAt", task.EnqueuedAt.Unix()),
	)
	
	return nil
}
