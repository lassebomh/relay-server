import { WebSocket, WebSocketServer } from "ws";
import z from "zod";

const Message = z.object({
  type: z.literal("subscribe").or(z.literal("unsubscribe")).or(z.literal("publish")),
  topic: z.string().max(1024),
  payload: z.optional(z.any()),
});

type Message = z.output<typeof Message>;

const wss = new WebSocketServer({ port: 8000 });
const subscriptions = new Map<string, Set<WebSocket>>();

wss.on("connection", (ws) => {
  const subscribedTopics = new Set<string>();

  ws.on("message", (data) => {
    let msg: Message;
    try {
      msg = Message.parse(JSON.parse(data.toString("utf8")));
    } catch {
      return;
    }

    let topic = subscriptions.get(msg.topic);
    if (!topic) {
      topic = new Set();
      subscriptions.set(msg.topic, topic);
    }

    if (msg.type === "subscribe") {
      topic.add(ws);
      subscribedTopics.add(msg.topic);
    }
    if (msg.type === "unsubscribe") {
      topic.delete(ws);
      subscribedTopics.delete(msg.topic);
      if (topic.size === 0) {
        subscriptions.delete(msg.topic);
      }
    }
    if (msg.type === "publish") {
      const outgoing = JSON.stringify({ topic: msg.topic, payload: msg.payload });
      for (const client of topic) {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          client.send(outgoing);
        }
      }
    }
  });

  ws.on("close", () => {
    for (const topicName of subscribedTopics) {
      const topic = subscriptions.get(topicName);
      if (!topic) continue;
      topic.delete(ws);
      if (topic.size === 0) {
        subscriptions.delete(topicName);
      }
    }
  });
});

console.log("Relay server listening on ws://localhost:8000");
