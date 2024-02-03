"use strict";

import { Client } from "@elastic/elasticsearch";
import type { ConnectionOptions, Client as EsClientType } from '@elastic/elasticsearch';
import { Db, Document, MongoClient, WithId } from "mongodb";
import { BulkOperationType, ErrorCause } from "@elastic/elasticsearch/lib/api/types";

interface Option {
  prefix: string;
  initialSync: boolean;
  debug: boolean;
  tls: ConnectionOptions['tls'];
}

export class Sync {
  db: Db;
  ESclient: EsClientType;
  mongoURL: string;
  elasticURL: string;
  option: Option = {
    prefix: "auto-sync-",
    initialSync: true,
    debug: false,
    tls:{
      ca: '',
      rejectUnauthorized: true,
    },
  };

  constructor(mongoURL: string, elasticURL: string, option: Option) {
    this.mongoURL = mongoURL;
    this.elasticURL = elasticURL;
    if (option) {
      this.option = option;
    }
  }

  async initialSync(excludedCollections?: string[]) {
    try {
      if (!this.ESclient) await this.initElastic();
      if (!this.db) await this.initMongo();
      if (this.option.debug) console.log("Debug: Initial mongodb sync started");
      await this.initDbSync(excludedCollections);
    } catch (error) {
      if (this.option.debug) throw error;
      else return;
    }
  }

  async startSync(excludedCollections?: string[]) {
    try {
      if (!this.ESclient) await this.initElastic();
      if (!this.db) await this.initMongo();
      if (this.option.initialSync) await this.initialSync(excludedCollections);
      await this.initWatcher();
    } catch (error) {
      if (this.option.debug) throw error;
      else return console.log(error);
    }
  }

  async startDropIndices(collectionsName: string[], excludedCollections?: string[]) {
    try {
      if (!this.ESclient) await this.initElastic();
      if (!this.db) await this.initMongo();
      await this.dropIndices(collectionsName, excludedCollections);
    } catch (error) {
      if (this.option.debug) throw error;
      else return console.log(error);
    }
  }

  private async initElastic() {
    try {
      const client = new Client({
        node: this.elasticURL,
        tls: this.option.tls,
      });
      if (!client) {
        throw new Error("Failed to connect elastic server");
      }
      if (this.option.debug) console.log("Debug: Connected to elastic");
      this.ESclient = client;
    } catch (error) {
      if (this.option.debug) console.log("Debug: Failed to connect elastic");
      throw error;
    }
  }

  private async initMongo() {
    try {
      const client = await MongoClient.connect(this.mongoURL);
      if (!client) {
        throw new Error("Failed to connect mongodb server");
      }
      if (this.option.debug) console.log("Debug: Connected to mongodb");
      this.db = client.db();
    } catch (error) {
      if (this.option.debug) console.log("Debug: Failed to connect mongodb");
      throw error;
    }
  }

  private getPrefix(prefix: string, collName: string){
    return prefix + '__' + collName.toLowerCase();
  }

  private async initDbSync(excludedCollections?: string[]) {
    try {
      let collectionsArr = await this.db.listCollections().toArray();
      let collectionsName = collectionsArr.map(ele =>
        ele.type === "collection" ? ele.name : null
      );
      for (let i = 0; i < collectionsName.length; i++) {
        let collName = collectionsName[i];
        // Skip indexing for some collections or if collName does not exist.
        if(!collName || (excludedCollections && excludedCollections.includes(collName))) continue;

        let index = this.getPrefix(this.option.prefix, collName);
        if (collName) {
          let allData = await this.db.collection(collName).find().toArray();
          await this.createBulkDataOnElastic(index, allData);
        }
      }
    } catch (error) {
      if (this.option.debug)
        console.log("Debug: Failed to initial sync mongodb");
      throw error;
    }
  }

  private async dropIndices(collectionsName: string[], excludedCollections?: string[]) {
    try {
      for (let i = 0; i < collectionsName.length; i++) {
        let collName = collectionsName[i];
        // Skip indexing for some collections or if collName does not exist.
        if(!collName || (excludedCollections && excludedCollections.includes(collName))) continue;

        let index = this.getPrefix(this.option.prefix, collName);
        if (collName) {
          await this.dropIndexOnElastic(index);
        }
      }
    } catch (error) {
      if (this.option.debug)
        console.log("Debug: Failed to drop indices in elastic");
      throw error;
    }
  }

  private async initWatcher() {
    try {
      return new Promise((resolve: any, reject: any) => {
        this.db
          .watch([], { fullDocument: "updateLookup" })
          .on("change", async (data: any, error: any) => {
            try {
              if (error) reject(error);
              if (this.option.debug)
                console.log("Debug: Change event triggered");
              await this.generateOperation(data);
              resolve();
            } catch (error) {
              if (this.option.debug)
                console.log("Debug: Error in change event");
              reject(error);
            }
          });
      });
    } catch (error) {
      if (this.option.debug) console.log("Debug: Error in change event");
      throw error;
    }
  }

  private async generateOperation(data: any) {
    try {
      let id, body;
      let index = this.getPrefix(this.option.prefix, data.ns.coll);

      console.log(data.operationType);
      switch (data.operationType) {
        case "delete":
          id = data.documentKey._id;
          await this.deleteDataOnElastic(id, index);
          break;

        case "insert":
          body = data.fullDocument;
          id = body?._id;
          if (id) {
            delete body._id;
          }
          await this.createDataOnElastic(id, index, body);
          break;

        case "update":
          body = data.fullDocument;
          id = body?._id;
          if (id) {
            delete body._id;
          }
          console.log(body);
          await this.updateDataOnElastic(id, index, body);
          break;

        case "drop":
          await this.dropIndexOnElastic(index);
          break;

        default:
          console.log(
            `ERROR: mongo-elastic-sync: Unhandled operation ${data.operationType}, log it here: https://github.com/souravj96/mongo-elastic-sync/issues`
          );
          break;
      }
    } catch (error) {
      throw error;
    }
  }

  private async dropIndexOnElastic(index: string) {
    try {
      await this.ESclient.indices.delete({
        index,
        ignore_unavailable: true,
      });

      if (this.option.debug) console.log("Debug: Elastic index dropped");
    } catch (error) {
      if (this.option.debug) console.log("Debug: Failed to drop index");
      throw error;
    }
  }

  private async deleteDataOnElastic(id: string, index: string) {
    try {
      await this.ESclient.delete({
        index,
        id,
      });

      if (this.option.debug) console.log("Debug: Elastic index deleted");
    } catch (error) {
      if (this.option.debug) console.log("Debug: Failed to delete index");
      throw error;
    }
  }

  private async updateDataOnElastic(id: string, index: string, body: object) {
    try {
      await this.ESclient.update({
        index: index,
        id: id,
        refresh: true,
        doc: body,
        doc_as_upsert: true,
      });

      if (this.option.debug) console.log("Debug: Elastic index updated");
    } catch (error) {
      if (this.option.debug) console.log("Debug: Failed to update index");
      throw error;
    }
  }

  private async createDataOnElastic(id: string, index: string, body: object) {
    try {
      await this.ESclient.index({
        index,
        id,
        document: body,
      });

      console.log(index,body);

      if (this.option.debug) console.log("Debug: Elastic index created");
    } catch (error) {
      if (this.option.debug) console.log("Debug: Failed to create index");
      throw error;
    }
  }

  private async createBulkDataOnElastic(index: string, body: WithId<Document>[]) {
    try {
      if (this.option.debug) console.log('\n', index);
      const data = body.flatMap(doc => {
        const {_id, ...rest} = doc;

        //_id should be string not an ObjectId for elastic
        let id = _id.toString();

        return [{ index: { _index: index, _id: id } }, rest];
      });

      if(!data.length) return console.error(`Index: ${index}\nError: No data available`)

      const bulkResponse = await this.ESclient.bulk({
        refresh: true,
        body: data,
      });

      if (bulkResponse.errors) {
        const erroredDocuments:{
          status?: number,
          error?: ErrorCause,
          operation: (typeof data)[number],
          document: (typeof data)[number],
        }[] = []
        // The items array has the same order of the dataset we just indexed.
        // The presence of the `error` key indicates that the operation
        // that we did for the document has failed.
        bulkResponse.items.forEach((action, i) => {
          const operation = Object.keys(action)[0] as BulkOperationType
          if (!!action[operation] && action[operation]?.error) {
            erroredDocuments.push({
              // If the status is 429 it means that you can retry the document,
              // otherwise it's very likely a mapping error, and you should
              // fix the document before to try it again.
              status: action[operation]?.status,
              error: action[operation]?.error,
              operation: data[i * 2],
              document: data[i * 2 + 1]
            })
          }
        })
        console.error('Errors in document while bulk: \n', erroredDocuments)
      }

      const { count } = await this.ESclient.count({ index })
      if (this.option.debug) console.log(
        `Number of documents from the collection db: ${body.length}\nThe number of documents indexed in the elastic: ${count}`
      )
      if(!(count === body.length)) console.error("The amount of data from the database and from elasticSearch is not the same, for INDEX:", index)

      if (this.option.debug)
        console.log("Debug: Elastic bulk index created: " + index);
    } catch (error) {
      if (this.option.debug) console.log("Debug: Failed to create bulk index");
      throw error;
    }
  }
}
