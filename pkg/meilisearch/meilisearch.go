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

func (client *Client) CreateIndex(indexUid, primaryKey string) error {
	//_, err := client.client.GetIndex(indexUid)
	//if err != nil {
	//	return err
	//}
	
	_, err := client.client.CreateIndex(&meilisearch.IndexConfig{
		Uid:        indexUid,
		PrimaryKey: primaryKey,
	})
	return err
}

func (client *Client) GetIndex(indexUid string) (*meilisearch.Index, error) {
	return client.client.GetIndex(indexUid)
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
		client.logger.Debug("更新meilisearch index属性成功",
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

// docs []map[string]interface{}

func (client *Client) CreateDocs(indexName string, docs interface{}, primaryKey ...string) error {
	
	index := client.client.Index(indexName)
	
	task, err := index.AddDocuments(docs)
	if err != nil {
		client.logger.Error("添加文档失败",
			zap.Error(err),
		)
		return err
	}
	
	client.logger.Debug("调用添加文档成功",
		zap.String("status", string(task.Status)),
		zap.String("indexUid", task.IndexUID),
		zap.String("type", string(task.Type)),
		zap.Int64("taskUid", task.TaskUID),
		zap.Int64("enqueuedAt", task.EnqueuedAt.Unix()),
	)
	
	return nil
}

func (client *Client) UpdateDocuments(indexName string, primaryKey string, document interface{}) error {
	index := client.client.Index(indexName)
	_, err := index.UpdateDocuments(document, primaryKey)
	return err
}

func (client *Client) DeleteDocument(indexName, identifier string) error {
	index := client.client.Index(indexName)
	_, err := index.DeleteDocument(identifier)
	return err
}
