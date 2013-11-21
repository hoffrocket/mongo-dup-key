package foo;

import com.mongodb.MongoException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.junit.Test;

public class DuplicateKeyTest 
{
    @Test(expected = MongoException.DuplicateKey.class)
    public void testThrowsKeyException() throws Exception
    {
        MongoClient mongo = new MongoClient("localhost:22020");
        DB test = mongo.getDB("test");
        test.dropDatabase();
        DB admin = mongo.getDB("admin");
        admin.command(new BasicDBObject("enableSharding", "test"));
        admin.command(new BasicDBObject("shardCollection", "test.t").append("key", new BasicDBObject("_id", 1)));
        admin.command(new BasicDBObject("split", "test.t").append("middle", new BasicDBObject("_id", 2)));
        admin.command(new BasicDBObject("moveChunk", "test.t")
                        .append("find", new BasicDBObject("_id", 2))
                        .append("to", "shard0001"));
        
        DBCollection coll = mongo.getDB("test").getCollection("t");
        coll.insert(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2));
        coll.insert(new BasicDBObject("_id", 1), new BasicDBObject("_id", 2));
    }
}
