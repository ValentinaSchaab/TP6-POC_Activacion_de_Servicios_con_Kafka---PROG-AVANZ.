import { Injectable, OnModuleInit } from "@nestjs/common";
import { MongoHolder } from "@poc/kafka-toolkit";
import { ClientSession } from "mongodb";
import { ActivationDoc } from "./activation.types";

@Injectable()
export class ActivationsRepository implements OnModuleInit {
  constructor(private readonly mongo: MongoHolder) {}

  async onModuleInit() {
    await this.collection().createIndex({ customerId: 1 });
  }

  collection() {
    return this.mongo.db.collection<ActivationDoc>("activations");
  }

  findById(id: string, session?: ClientSession) {
    return this.collection().findOne({ _id: id }, { session });
  }
}
