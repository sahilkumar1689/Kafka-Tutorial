const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "payment-group" });
const producer = kafka.producer();

const MAIN_TOPIC = "payment-topic";
const RETRY_TOPIC = "payment-retry";
const DLQ_TOPIC = "payment-dlq";

const MAX_RETRIES = 3;

// Simulated business logic
async function processPayment(data) {
  console.log("💳 Processing:", data);

  // Random failure simulation
  if (Math.random() < 0.5) {
    throw new Error("Random payment failure");
  }

  console.log("✅ Success:", data);
}

// Retry delay (simple)
function getRetryDelay(retryCount) {
  return retryCount * 5000; // 5s, 10s, 15s...
}

async function run() {
  await consumer.connect();
  await producer.connect();

  // Subscribe to BOTH topics
  await consumer.subscribe({ topic: MAIN_TOPIC, fromBeginning: true });
  await consumer.subscribe({ topic: RETRY_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value.toString();
      let data;

      try {
        data = JSON.parse(value);
      } catch {
        data = { payload: value };
      }

      const retryCount = parseInt(message.headers?.retryCount?.toString() || "0");

      try {
        // ⏳ Retry delay handling
        if (data.retryAt && Date.now() < data.retryAt) {
          console.log("⏳ Skipping until retry time...");
          return;
        }

        await processPayment(data.payload);

      } catch (error) {
        console.log("❌ Failed:", error.message);

        if (retryCount >= MAX_RETRIES) {
          // 👉 Send to DLQ
          await producer.send({
            topic: DLQ_TOPIC,
            messages: [
              {
                value: JSON.stringify({
                  ...data,
                  error: error.message,
                }),
              },
            ],
          });

          console.log("☠️ Sent to DLQ");
        } else {
          // 👉 Send to retry topic
          const nextRetry = retryCount + 1;

          await producer.send({
            topic: RETRY_TOPIC,
            messages: [
              {
                value: JSON.stringify({
                  payload: data.payload,
                  retryAt: Date.now() + getRetryDelay(nextRetry),
                }),
                headers: {
                  retryCount: String(nextRetry),
                },
              },
            ],
          });

          console.log(`🔁 Sent to retry (${nextRetry})`);
        }
      }
    },
  });
}

run().catch(console.error);