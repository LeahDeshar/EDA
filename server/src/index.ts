import express, { NextFunction, Request, Response } from "express";
import morgan from "morgan";
import dotenv from "dotenv";
import cors from "cors";
import helmet from "helmet";
import logger from "./config/logger";
import limiter from "./middleware/rateLimit";
import client, { initRedisClient } from "./config/redis";
import { Repository } from "redis-om";
import { Schema } from "redis-om";
import { promisify } from "util";
// import  { Client, Entity, Schema } from 'redis/om';
export const app = express();
app.use(express.json());
app.use(morgan("dev"));
app.use(cors());
app.use(helmet());
app.use(limiter);

dotenv.config();

initRedisClient();
const getAsync = promisify(client.get).bind(client);
const setAsync = promisify(client.set).bind(client);
const deliverySchema = new Schema("Delivery", {
  budget: { type: "number" },
  notes: { type: "string" },
});
const deliveryRepository = new Repository(deliverySchema, client);
deliveryRepository.createIndex();

const eventSchema = new Schema("Event", {
  deliveryId: { type: "string" },
  type: { type: "string" },
  data: { type: "string" },
});
const eventRepository = new Repository(eventSchema, client);
eventRepository.createIndex();
interface BudgetState {
  budget: number;
  currency: string;
  notes?: string;
}

interface BudgetUpdatedEvent {
  type: "BUDGET_UPDATED";
  payload: {
    budget: number;
  };
}
interface NoteUpdatedEvent {
  type: "NOTE_UPDATED";
  payload: {
    notes: string;
  };
}

const CONSUMERS = {
  BUDGET_UPDATED: (state: BudgetState, event: BudgetUpdatedEvent) => {
    state.budget = event.payload.budget;
    return state;
  },
  NOTE_UPDATED: (state: BudgetState, event: NoteUpdatedEvent) => {
    state.notes = event.payload.notes;
    return state;
  },
};

const PORT = process.env.PORT || 8083;

app.get("/", (req, res) => {
  logger.info("Root route accessed");
  res.send("Welcome to the API!");
});
app.get("/deliveries/:pk/status", async (req: Request, res: Response) => {
  const { pk } = req.params;
  let state = await getAsync(`delivery:${pk}`);

  if (state) {
    return res.json(JSON.parse(state));
  }

  state = await buildState(pk);
  await setAsync(`delivery:${pk}`, JSON.stringify(state));
  res.json(state);
});

async function buildState(pk: string) {
  const event = await eventRepository.search().return.all();
  const pks: string[] = event.map((event) => event.pk);

  const allEvents = await Promise.all(
    pks.map((pk: string) => eventRepository.fetch(pk))
  );
  const events = allEvents.filter((event) => event.deliveryId === pk);
  let state: BudgetState = { budget: 0, currency: "USD" };

  for (const event of events) {
    state = CONSUMERS[event.type as keyof typeof CONSUMERS](state, event);
  }

  return state;
}
app.listen(PORT, () => {
  logger.info(`Server is running on port ${PORT}`);
});
