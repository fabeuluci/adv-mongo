import { MongoDbManager } from "./MongoDbManager";
import { Logger } from "adv-log";
import * as mongodb from "mongodb";

export type MigrationStatus = "PERFORMING"|"FAIL"|"SUCCESS";

export interface MigrationModel {
    id: string;
    startDate: number;
    endDate: number|null;
    status: MigrationStatus;
}

export interface IMigration {
    go: () => Promise<void>;
}

export interface MigrationConstructor {
    id: string;
    new(...args: any[]): IMigration;
}

export interface Migration {
    id: string;
    create(session: mongodb.ClientSession): IMigration;
}

export class MigrationManager {
    
    constructor(
        private mongoDbManager: MongoDbManager,
        private logger: Logger,
        private migrations: Migration[]
    ) {
    }
    
    getRepository() {
        return this.mongoDbManager.getRepository<string, MigrationModel>("migration");
    }
    
    async go() {
        this.logger.debug("Starting migration process...");
        const repo = await this.getRepository();
        const migrationModels = await repo.getAll();
        if (migrationModels.find(x => x.status != "SUCCESS")) {
            this.logger.error("Old migrations not finished with success. Repair db state manually");
            throw new Error("Old migrations not finished with success. Repair db state manually");
        }
        for (const migration of this.migrations) {
            const mModel = migrationModels.find(x => x.id == migration.id);
            if (mModel) {
                this.logger.debug("Migration '" + migration.id + "' already done!");
                continue;
            }
            this.logger.debug("Performing '" + migration.id + "' migration...");
            const model: MigrationModel = {
                id: migration.id,
                startDate: Date.now(),
                endDate: null,
                status: "PERFORMING"
            };
            await repo.insert(model);
            try {
                await this.mongoDbManager.withTransaction(async session => {
                    const m = migration.create(session);
                    await m.go();
                });
                model.endDate = Date.now();
                model.status = "SUCCESS";
                await repo.update(model);
                this.logger.debug("Migration '" + migration.id + "' sucessfully finished!");
            }
            catch (e) {
                this.logger.error("Error during performing migration '" + migration.id + "'", e);
                model.endDate = Date.now();
                model.status = "FAIL";
                await repo.update(model);
                this.logger.error("Migration process fails!");
                throw new Error("Migration process fails!");
            }
        }
        this.logger.debug("Migration process finish with success!");
    }
}