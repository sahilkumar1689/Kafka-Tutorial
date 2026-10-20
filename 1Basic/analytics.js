// analytics.js
import { Kafka } from "kafkajs";
const kafka = new Kafka({ clientId: "analytics-service", brokers: ["localhost:9092"] });

const topic = "match-events";
const groupId = process.env.GROUP || "analytics-group";

(async () => {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

    //  1. fromBeginning only applies when no offset exists for a consumer group; after that, Kafka always resumes from the last committed offset.
    // 2. true = only for first run, then it will resume from last committed offset.
    // 3. false = start consuming from the end of the topic, ignoring all existing messages.
    // 4. seek() = manually set the offset to consume from, allowing you to reprocess messages or skip ahead as needed.
    // e.g. =  consumer.seek({
    //   topic: "match-events",
    //   partition: 0,
    //   offset: "0"
    // });

    // 5. By default autocommit is true so the offset commit is handled by the kafka internally.But by make it false we can manully set the offset like this:
    // e.g. = await consumer.commitOffsets([
    //   {
    //     topic: "match-events",
    //     partition: 0,
    //     offset: "5"
    //   }
    // ]);

    // Delivery guarantees:
    // 1. At most once: messages may be lost but are never redelivered. (autoCommit=true, commit happens before processing)
    // 2. At least once: messages are never lost but may be redelivered. (autoCommit=true, commit happens after processing)
    // 3. Exactly once: messages are processed exactly once with no duplicates or losses. (requires idempotent producer and transactional consumer)

    // Note = 1. __consumer_offsets = It is the special topic where Kafka stores the committed offsets for each consumer group. It is an internal topic and should not be used for application data.

    // 2. Kafka stores offset per partition and per consumer group.If you need to consumer the old data you have add new consumer group or you can seek to the beginning of the topic or you can reset from the CLI.

    // 3. Kafka by default store data for 7 days i.e. KAFKA_LOG_RETENTION_HOURS: 168 hours.There are two ways to set the retention i.e. time based log.retention.hours=168 and size based retention log.retention.bytes=1073741824

    // 4. In kafka we implement the retry and the dead letter queue mechanism at the topic level i.e. we can create a separate retry topic and a dead letter topic and then we can use the consumer to consume from the retry topic and if the message is failed again then we can send it to the dead letter topic.

    // 5. Here is the actuall flow. Producers -> payments,orders,deliveries topic -> their respective consumers groups.After that we also have retry-payments,retry-orders,retry-deliveries topic and those are connected with already existing consumer groups.Finally we have dead-letter-payments,dead-letter-orders,dead-letter-deliveries topics also.

    // Now, each consumer is generic like they subscribe to both the main topic and the retry topic.They simple check:
    // case 1 = if failed message comes then check the retryAt time whether its the correct time to process it or not.If yes then process it otherwise ignore it and wait for the next message.If the message is processed successfully then commit the offset otherwise check the retry count if it is less than the max retry count then send it to the retry topic with the retryAt time otherwise send it to the dead letter topic.
    // Case 2 = if the message is from the main topic then simply process it and if it is failed then check the retry count if it is less than the max retry count then send it to the retry topic with the retryAt time otherwise send it to the dead letter topic.

    // 6. Kafka uses a custom binary protocol over TCP and follows a pull-based model where consumers fetch messages, unlike RabbitMQ which uses AMQP and a push-based delivery model.

    // 7. TCP is a Transport Layer protocol (OSI Layer 4) responsible for reliable, ordered, and error-checked delivery of data between two endpoints. It establishes a connection using a 3-way handshake (SYN → SYN-ACK → ACK), ensures packets arrive in sequence, retransmits lost packets, and manages flow control. TCP itself does not know or care about the meaning of the data — it just ensures it gets delivered correctly.
    // HTTPS is an Application Layer protocol (OSI Layer 7) built on top of HTTP but with TLS/SSL encryption for security. It defines how web browsers and servers exchange data, ensuring confidentiality, integrity, and authentication. HTTPS uses TCP as its transport mechanism, meaning all HTTPS traffic benefits from TCP’s reliability while adding encryption.
    // In short, TCP is the road, and HTTPS is the secure vehicle traveling on it — you need TCP’s reliability for HTTPS to function securely and effectively.
    
    // 8. HTTP is an application-layer protocol that relies on TCP (a transport-layer protocol) to ensure reliable, ordered delivery of data. Without TCP (or another transport mechanism), HTTP wouldn’t work as intended.
    // On the other hand, TCP is independent — it doesn’t “need” HTTP or HTTPS. Many systems use TCP directly or build their own higher-level protocols on top of it.

    // For example:
    // . Kafka indeed uses a custom binary protocol over TCP. Instead of HTTP, it defines its own message framing, serialization, and request/response handling to optimize for high-throughput, low-latency communication between brokers, producers, and consumers.
    // 2. Other examples include SMTP (email), FTP (file transfer), and SSH — all of which run over TCP but are not HTTP.


  const stats = {}; // { matchId: { batsman: runs } }

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      const { matchId, matchName, batsman, runs, isWicket } = JSON.parse(message.value.toString());
      if (!stats[matchId]) stats[matchId] = {};
      stats[matchId][batsman] = (stats[matchId][batsman] || 0) + (isWicket ? 0 : runs);

      console.log(`📈 [ANALYTICS][P${partition}] ${matchName} → ${JSON.stringify(stats[matchId])}`);
    },
  });
})();
