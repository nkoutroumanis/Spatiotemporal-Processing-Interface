package gr.ds.unipi.stpin.outputs;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

final class MongoDbConnector {

    private final MongoClient mongoClient;
    private final String database;

    private MongoDbConnector(String host, int port, String database, String username, String password) {
        MongoCredential credential = MongoCredential.createCredential(username, "admin", password.toCharArray());//admin for global

        MongoClientSettings clientSettings = MongoClientSettings.builder().credential(credential).applyToClusterSettings(builder ->
                builder.hosts(Arrays.asList(new ServerAddress(host, port))))
                .applyToConnectionPoolSettings(builder -> builder.maxConnectionIdleTime(90000, TimeUnit.SECONDS)).build();

        mongoClient = MongoClients.create(clientSettings);

        this.database = database;
    }

    public static MongoDbConnector newMongoDbConnector(String host, int port, String database, String username, String password) {
        return new MongoDbConnector(host, port, database, username, password);
    }

    public String getDatabase() {
        return database;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }


}
