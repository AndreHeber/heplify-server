package database

import (
	"context"
	"time"

	"github.com/negbie/logp"
	"github.com/sipcapture/heplify-server/config"
	"github.com/sipcapture/heplify-server/decoder"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDB struct {
    client   *mongo.Client
    database *mongo.Database
    bulkCnt  int
}

func (m *MongoDB) setup() error {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    clientOptions := options.Client().ApplyURI(config.Setting.DBAddr)
    client, err := mongo.Connect(ctx, clientOptions)
    if err != nil {
        return err
    }

    if err := client.Ping(ctx, nil); err != nil {
        return err
    }

    m.client = client
    m.database = client.Database(config.Setting.DBDataTable)
    m.bulkCnt = config.Setting.DBBulk
    if m.bulkCnt < 1 {
        m.bulkCnt = 1
    }

    logp.Info("MongoDB connection established\n")
    return nil
}

func (m *MongoDB) insert(hCh chan *decoder.HEP) {
    collection := m.database.Collection("hep_packets")
    bulkOps := make([]mongo.WriteModel, 0, m.bulkCnt)

    for pkt := range hCh {
        doc := bson.M{
            "sid":             pkt.SID,
            "create_date":     pkt.Timestamp,
            "protocol_header": pkt.ProtoType,
            "data_header":     pkt.Payload,
            "raw":             pkt.Raw,
        }
        bulkOps = append(bulkOps, mongo.NewInsertOneModel().SetDocument(doc))

        if len(bulkOps) == m.bulkCnt {
            _, err := collection.BulkWrite(context.Background(), bulkOps)
            if err != nil {
                logp.Err("MongoDB bulk insert error: %v", err)
            }
            bulkOps = bulkOps[:0]
        }
    }

    if len(bulkOps) > 0 {
        _, err := collection.BulkWrite(context.Background(), bulkOps)
        if err != nil {
            logp.Err("MongoDB bulk insert error: %v", err)
        }
    }
}