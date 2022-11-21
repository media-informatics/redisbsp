package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Album struct {
	ID     primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Artist string             `bson:"artist,omitempty" json:"artist,omitempty"`
	Title  string             `bson:"album,omitempty" json:"album,omitempty"`
	Year   int                `bson:"year,omitempty" json:"year:omitempty"`
}

type Albums []Album

type ObjID struct {
	Value primitive.ObjectID `bson:"_id" json:"_id"`
}

func (oid *ObjID) Hex() string {
	return oid.Value.Hex()
}

var cache *redis.Client
var myAlbums *mongo.Collection

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opt := options.Client().ApplyURI("mongodb://root:rootpassword@localhost:27017")
	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		log.Fatal(err)
	}
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatal(err)
	}
	myAlbums = client.Database("mydb").Collection("albums")

	cache = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no passwd
		DB:       0,  // default DB
	})

	res := cache.Ping(ctx)
	if err := res.Err(); err != nil {
		log.Fatalf("connection to redis failed: %w", err)
	}
	// Create
	initMongo(ctx)

	// wählt as Suchergebnissen nur die _id aus:
	ids, err := allIds(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// erste schleife: redis-cache enthält noch keine Daten
	copyToCache(ctx, ids, 1*time.Second)
	readCache(ctx, ids)
	time.Sleep(2 * time.Second)
	readCache(ctx, ids) // cache sollte leer sein

	myAlbums.Drop(ctx)
}

func initMongo(ctx context.Context) {
	entries := []interface{}{
		bson.D{{"artist", "Rammstein"}, {"album", "Zeit"}, {"year", 2022}},
		bson.D{{"artist", "Queen"}, {"album", "A Day at the Races"}, {"year", 1976}},
		bson.D{{"artist", "Beethoven"}, {"album", "9. Symphonie"}, {"year", 1824}},
		bson.D{{"artist", "Rammstein"}, {"album", "Rammstein"}, {"year", 2019}},
	}
	_, err := myAlbums.InsertMany(ctx, entries)
	if err != nil {
		log.Fatalf("could not insert entries %v: %v", entries, err)
	}
}

func readCache(ctx context.Context, ids []ObjID) {
	for i, id := range ids {
		val, err := cache.Get(ctx, id.Hex()).Result()
		if err != nil {
			log.Printf("2nd cache: %w", err)
			continue
		}
		var a Album
		if err = json.Unmarshal([]byte(val), &a); err != nil {
			log.Printf("unmarshal error at %d: %w", i, err)
		}
		fmt.Printf("cache2: %d: %v %v\n", i, id.Hex(), a)
	}

}

func copyToCache(ctx context.Context, ids []ObjID, drop time.Duration) {
	for i, id := range ids {
		val, err := cache.Get(ctx, id.Hex()).Result()
		if err != nil { // nicht gefunden: hole aus DB und füge in cache ein
			var a Album
			entry := myAlbums.FindOne(ctx, bson.M{"_id": id.Value})
			entry.Decode(&a)          // von BSON nach Go
			j, err := json.Marshal(a) // von Go nach JSON
			if err != nil {
				log.Print(err)
			}
			// füge als JSON in Redis ein. Lösche nach n Sekunden
			cache.SetEx(ctx, id.Hex(), j, drop)
			continue
		}
		fmt.Printf("cache: %d: %v %v\n", i, id.Hex(), val)
	}
}

func allIds(ctx context.Context) ([]ObjID, error) {
	findOptions := options.Find().SetProjection(bson.M{"_id": 1})
	allIds, err := myAlbums.Find(ctx, bson.D{}, findOptions)
	if err != nil {
		return nil, err
	}

	var ids []ObjID
	err = allIds.All(ctx, &ids)
	if err != nil {
		return nil, err
	}
	return ids, err
}
