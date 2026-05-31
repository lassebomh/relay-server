import { WebSocket, WebSocketServer } from "ws";
import z from "zod";
import * as net from "net";

function getAddressGroup(remoteAddress: string): string | undefined {
  const cleaned = remoteAddress.replace(/^::ffff:/, "");
  if (net.isIPv4(cleaned)) {
    return cleaned;
  }

  if (net.isIPv6(cleaned)) {
    let segments: string[];
    const halves = cleaned.split("::");
    if (halves.length === 2) {
      const left = halves[0] ? halves[0].split(":") : [];
      const right = halves[1] ? halves[1].split(":") : [];
      const missing = 8 - left.length - right.length;
      segments = [...left, ...Array(missing).fill("0"), ...right];
    } else {
      segments = cleaned.split(":");
    }
    return segments
      .map((s) => s || "0")
      .slice(0, 4)
      .join(":");
  }

  return undefined;
}

const Message = z.object({
  type: z.literal("subscribe").or(z.literal("unsubscribe")).or(z.literal("publish")),
  topic: z.string().max(1024),
  payload: z.optional(z.any()),
});

type Message = z.output<typeof Message>;

const wss = new WebSocketServer({ port: 8000 });
const subscriptions = new Map<string, Set<WebSocket>>();
const lanTopicPrefix = crypto.randomUUID();

wss.on("connection", (ws, request) => {
  const subscribedTopics = new Set<string>();
  const addressGroup = getAddressGroup(request.socket.remoteAddress || "");

  if (!addressGroup) {
    ws.close();
    return;
  }

  const lanTopic = lanTopicPrefix + addressGroup;

  ws.on("message", (data) => {
    let msg: Message;
    try {
      msg = Message.parse(JSON.parse(data.toString("utf8")));
    } catch {
      return;
    }

    const topicName = msg.topic === "lan" ? lanTopic : msg.topic;
    const topic = subscriptions.get(topicName);

    if (msg.type === "subscribe") {
      subscribedTopics.add(topicName);

      if (topic !== undefined) {
        topic.add(ws);
      } else {
        subscriptions.set(topicName, new Set([ws]));
      }
    } else if (msg.type === "unsubscribe") {
      subscribedTopics.delete(topicName);

      if (topic !== undefined) {
        topic.delete(ws);
        if (topic.size === 0) {
          subscriptions.delete(topicName);
        }
      }
    } else if (msg.type === "publish" && topic !== undefined) {
      const outgoing = JSON.stringify({
        topic: msg.topic, // Use provided topic name, so we dont send the address group
        payload: msg.payload,
      });
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
