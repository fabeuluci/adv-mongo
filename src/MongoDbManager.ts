import { PromiseUtils } from "adv-promise";
import * as mongodb from "mongodb";
import { MongoObjectRepository } from "./ObjectRepository";

export interface MongoConfig {
    url: string;
    dbName: string;
}

export class MongoDbManager {
    
    constructor(
        private client: mongodb.MongoClient,
        private db: mongodb.Db,
        private collectionsMap: {[name: string]: Promise<mongodb.Collection>},
        private indexes: {[collectionName: string]: string[]},
        private idProperties: {[collectionName: string]: string},
    ) {
    }
    
    static async init(config: MongoConfig, collectionsMap: {[name: string]: Promise<mongodb.Collection>}, indexes: {[collectionName: string]: string[]}, idProperties: {[collectionName: string]: string}): Promise<MongoDbManager> {
        const client = await mongodb.MongoClient.connect(config.url, {minPoolSize: 5, maxPoolSize: 5});
        const db = client.db(config.dbName);
        const dbManager = new MongoDbManager(client, db, collectionsMap, indexes, idProperties);
        const collections: {name: string, type?: string}[] = await db.listCollections().toArray();
        for (const colName in dbManager.idProperties) {
            if (collections.find(x => x.name == colName) == null) {
                await db.createCollection(colName, {});
            }
            await dbManager.getCollection(colName);
        }
        return dbManager;
    }
    
    async getCollection(collectionName: string): Promise<mongodb.Collection> {
        if (!(collectionName in this.collectionsMap)) {
            const collection = this.db.collection(collectionName);
            if (!(collectionName in this.indexes)) {
                return this.collectionsMap[collectionName] = Promise.resolve(collection);
            }
            return this.collectionsMap[collectionName] = Promise.resolve().then(async () => {
                for (const index of this.indexes[collectionName]) {
                    await collection.createIndex(index);
                }
                return collection;
            });
        }
        return this.collectionsMap[collectionName];
    }
    
    removeCollection(collectionName: string): Promise<boolean> {
        return this.db.collection(collectionName).drop();
    }
    
    nextId() {
        return new mongodb.ObjectId().toHexString();
    }
    
    async getRepository<K extends string|number, V>(collectionName: string, session?: mongodb.ClientSession): Promise<MongoObjectRepository<K, V>> {
        if (!(collectionName in this.idProperties)) {
            throw new Error("There is no id property for collection " + collectionName);
        }
        return new MongoObjectRepository(await this.getCollection(collectionName), <keyof V>this.idProperties[collectionName], session);
    }
    
    async withTransaction<T>(func: (session: mongodb.ClientSession) => Promise<T>): Promise<T> {
        const session = this.client.startSession();
        try {
            let res: T|null = null;
            await session.withTransaction(async () => {
                res = await func(session);
            }, {
                readPreference: new mongodb.ReadPreference("primary"),
                readConcern: new mongodb.ReadConcern("local"),
                writeConcern: new mongodb.WriteConcern("majority")
            });
            return <T><unknown>res;
        }
        finally {
            await PromiseUtils.promisify(x => session.endSession(x));
        }
    }
    
    close() {
        return this.client.close();
    }
}
