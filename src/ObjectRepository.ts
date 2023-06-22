import * as mongodb from "mongodb";
import { Query, QueryResult } from "./Query";

export interface ObjectRepositoryRead<K extends string|number, V> {
    get(key: K): Promise<V|null>;
    getMany(keys: K[]): Promise<V[]>;
    getManyAsMap(keys: K[]): Promise<Map<K, V>>;
    getOrDefault(key: K, def: V): Promise<V>;
    getAll(): Promise<V[]>;
    count(f: (q: Query<V>) => QueryResult): Promise<number>;
    find(f: (q: Query<V>) => QueryResult): Promise<V|null>;
    findAll(f: (q: Query<V>) => QueryResult): Promise<V[]>;
    exists(key: K): Promise<boolean>;
    query(f: (q: Query<V>) => QueryResult): ObjectQuery<V>;
}

export interface ObjectRepository<K extends string|number, V> extends ObjectRepositoryRead<K, V> {
    insert(value: V): Promise<void>;
    replace(value: V): Promise<void>;
    update(value: V): Promise<void>;
    delete(key: K): Promise<boolean>;
    deleteMany(f: (q: Query<V>) => QueryResult): Promise<number>;
}

export interface ObjectQuery<T> {
    one(): Promise<T|null>;
    array(): Promise<T[]>;
    count(): Promise<number>;
    exists(): Promise<boolean>;
    limit(limit: number): ObjectQuery<T>;
    skip(skip: number): ObjectQuery<T>;
    sort(field: keyof T, asc: boolean): ObjectQuery<T>;
}

export class MongoObjectQuery<T> implements ObjectQuery<T> {
    
    private limitValue?: number;
    private skipValue?: number;
    private sortValue?: {field: string, asc: boolean};
    
    constructor(
        private collection: mongodb.Collection,
        private query: QueryResult,
        private convertFromDb: (x: unknown) => T,
        private session?: mongodb.ClientSession
    ) {
    }
    
    private getOptions<Z extends {session?: mongodb.ClientSession} = any>(opt?: Z): Z {
        opt = opt || <Z>{};
        if (this.session) {
            opt.session = this.session;
        }
        return opt;
    }
    
    private prepare(addSession: boolean) {
        let cursor = this.collection.find(this.query, addSession ? this.getOptions() : {});
        if (this.limitValue != null) {
            cursor = cursor.limit(this.limitValue);
        }
        if (this.skipValue != null) {
            cursor = cursor.skip(this.skipValue);
        }
        if (this.sortValue != null) {
            const obj: any = {};
            obj[this.sortValue.field] = this.sortValue.asc ? 1 : -1;
            cursor = cursor.sort(obj);
        }
        return cursor;
    }
    
    async one(): Promise<T|null> {
        const list = await this.limit(1).prepare(true).toArray();
        return list.length > 0 ? this.convertFromDb(list[0]) : null;
    }
    
    async array(): Promise<T[]> {
        const list = await this.prepare(true).toArray();
        return list.map(x => this.convertFromDb(x));
    }
    
    count(): Promise<number> {
        return this.prepare(false).count();
    }
    
    async exists(): Promise<boolean> {
        const res = await this.count();
        return res > 0;
    }
    
    limit(limit: number): MongoObjectQuery<T> {
        this.limitValue = limit;
        return this;
    }
    
    skip(skip: number): MongoObjectQuery<T> {
        this.skipValue = skip;
        return this;
    }
    
    sort(field: keyof T, asc: boolean): MongoObjectQuery<T> {
        this.sortValue = {field: <string>field, asc: asc};
        return this;
    }
}

export class MongoObjectRepository<K extends string|number, V> implements ObjectRepository<K, V> {
    
    constructor(
        public readonly collection: mongodb.Collection,
        private idProperty: keyof V,
        private session?: mongodb.ClientSession
    ) {
    }
    
    generateId() {
        return new mongodb.ObjectId().toHexString();
    }
    
    private convertFromDbObj(dbObj: unknown): V {
        (<any>dbObj)[this.idProperty] = (<any>dbObj)._id;
        delete (<any>dbObj)._id;
        return <V>dbObj;
    }
    
    private convertToDbObj(obj: V): V {
        const dbObj: any = {};
        for (const key in <any>obj) {
            dbObj[key == this.idProperty ? "_id" : key] = (<any>obj)[key];
        }
        return dbObj;
    }
    
    private getOptions<T extends {session?: mongodb.ClientSession} = any>(opt?: T): T {
        opt = opt || <T>{};
        if (this.session) {
            opt.session = this.session;
        }
        return opt;
    }
    
    async get(key: K): Promise<V|null> {
        const x = await this.collection.findOne({ _id: key }, this.getOptions());
        return x ? this.convertFromDbObj(x) : null;
    }
    
    async getMany(keys: K[]): Promise<V[]> {
        const res = await this.collection.find({_id: {$in: keys}}, this.getOptions()).toArray();
        return res.map(x => this.convertFromDbObj(x));
    }
    
    async getManyAsMap(keys: K[]): Promise<Map<K, V>> {
        const list = await this.getMany([...new Set(keys.filter(x => x != null))]);
        const map = new Map<K, V>();
        for (const entry of list) {
            map.set((<any>entry)[this.idProperty], entry);
        }
        return map;
    }
    
    async getOrDefault(key: K, def: V): Promise<V> {
        const x = await this.collection.findOne({ _id: key }, this.getOptions());
        return x ? this.convertFromDbObj(x) : def;
    }
    
    async getAll(): Promise<V[]> {
        const res = await this.collection.find({}, this.getOptions()).toArray();
        return res.map(x => this.convertFromDbObj(x));
    }
    
    count(f: (q: Query<V>) => QueryResult): Promise<number> {
        const query = f(new Query(this.idProperty));
        return this.collection.find(query, {}).count();
    }
    
    async find(f: (q: Query<V>) => QueryResult): Promise<V|null> {
        const query = f(new Query(this.idProperty));
        const x = await this.collection.findOne(query, this.getOptions());
        return x ? this.convertFromDbObj(x) : null;
    }
    
    async findAll(f: (q: Query<V>) => QueryResult): Promise<V[]> {
        const query = f(new Query(this.idProperty));
        const res = await this.collection.find(query, this.getOptions()).toArray();
        return res.map(x => this.convertFromDbObj(x));
    }
    
    async exists(key: K): Promise<boolean> {
        return (await this.get(key)) != null;
    }
    
    async insert(value: V): Promise<void> {
        if (value[this.idProperty] == null) {
            value[this.idProperty] = <any>(new mongodb.ObjectId()).toHexString();
        }
        await this.collection.insertOne(this.convertToDbObj(value), this.getOptions());
    }
    
    async replace(value: V): Promise<void> {
        await this.collection.replaceOne({ _id: value[this.idProperty] }, this.convertToDbObj(value), this.getOptions());
    }
    
    async update(value: V): Promise<void> {
        await this.collection.replaceOne({ _id: value[this.idProperty] }, this.convertToDbObj(value), this.getOptions<mongodb.ReplaceOptions>({upsert: true}));
    }
    
    async updateMany(f: (q: Query<V>) => QueryResult, update: Partial<V>): Promise<void> {
        const query = f(new Query(this.idProperty));
        await this.collection.updateMany(query, {$set: update}, this.getOptions());
    }
    
    async delete(key: K): Promise<boolean> {
        const res = await this.collection.deleteOne({ _id: key }, this.getOptions());
        return res.deletedCount > 0;
    }
    
    async deleteMany(f: (q: Query<V>) => QueryResult): Promise<number> {
        const query = f(new Query(this.idProperty));
        const res = await this.collection.deleteMany(query, this.getOptions());
        return res.deletedCount;
    }
    
    query(f: (q: Query<V>) => QueryResult): ObjectQuery<V> {
        return new MongoObjectQuery(this.collection, f(new Query(this.idProperty)), this.convertFromDbObj.bind(this), this.session);
    }
}