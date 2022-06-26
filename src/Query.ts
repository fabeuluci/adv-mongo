export interface QueryResult {
    [key: string]: any;
    __queryResult: never;
}

type ArrayElement<A> = A extends (infer T)[] ? T : never;

export class Query<T> {
    
    constructor(private idProperty: keyof T, private baseName: string = "") {
    }
    
    empty(): QueryResult {
        return <QueryResult>{};
    }
    
    not(query: QueryResult): QueryResult {
        const res = <QueryResult>{};
        for (const key in query) {
            res[key] = {$not: query[key]};
        }
        return res;
    }
    
    getPropName(prop: keyof T): string {
        if (prop == this.idProperty) {
            return "_id";
        }
        return (this.baseName ? this.baseName + "." : "") + <string>prop;
    }
    
    eq<K extends keyof T>(prop: K, value: T[K]): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = value;
        return res;
    }
    
    neq<K extends keyof T>(prop: K, value: T[K]): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = {$ne: value};
        return res;
    }
    
    null<K extends keyof T>(prop: K): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = null;
        return res;
    }
    
    exists<K extends keyof T>(prop: K): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = {$exists: true};
        return res;
    }
    
    notExists<K extends keyof T>(prop: K): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = {$exists: false};
        return res;
    }
    
    regex<K extends keyof T>(prop: K, regex: string|RegExp): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = {$regex: regex};
        return res;
    }
    
    gt<K extends keyof T>(prop: K, value: T[K]): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = {$gt: value};
        return res;
    }
    
    gte<K extends keyof T>(prop: K, value: T[K]): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = {$gte: value};
        return res;
    }
    
    lt<K extends keyof T>(prop: K, value: T[K]): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = {$lt: value};
        return res;
    }
    
    lte<K extends keyof T>(prop: K, value: T[K]): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = {$lte: value};
        return res;
    }
    
    includes<K extends keyof T>(prop: K, value: ArrayElement<T[K]>): QueryResult {
        const res = <QueryResult>{};
        res[this.getPropName(prop)] = value;
        return res;
    }
    
    and(...args: QueryResult[]): QueryResult {
        const res = <QueryResult>{};
        res["$and"] = args;
        return res;
    }
    
    or(...args: QueryResult[]): QueryResult {
        const res = <QueryResult>{};
        res["$or"] = args;
        return res;
    }
    
    prop<K extends keyof T>(prop: K): Query<T[K]> {
        return new Query(<any>"", this.getPropName(prop));
    }
    
    arrayProp<K extends keyof T>(prop: K): Query<ArrayElement<T[K]>> {
        return new Query(<any>"", this.getPropName(prop));
    }
}
