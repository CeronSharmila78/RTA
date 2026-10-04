/**
 * Apache Kafka Multi-Machine Simulation Engine
 * Simulates KRaft Broker, Producer, Consumer, Storage Log, and Network Conditions.
 */

class KafkaVirtualLab {
  constructor() {
    // Machine States
    this.machine1 = {
      name: "Machine 1 (Kafka Broker)",
      ip: "192.168.1.10",
      jdkInstalled: true,
      kafkaInstalled: true,
      clusterId: null,
      storageFormatted: false,
      isRunning: false,
      listeners: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
      advertisedListeners: "PLAINTEXT://192.168.1.10:9092", // Default correct
      firewallPort9092Allowed: true,
      topics: {},
      logDirs: "C:/tmp/kraft-combined-logs",
      brokerId: 1,
      totalCommittedMessages: 0
    };

    this.machine2 = {
      name: "Machine 2 (Producer)",
      ip: "192.168.1.20",
      jdkInstalled: true,
      kafkaInstalled: true,
      isProducing: false,
      activeTopic: null,
      messagesSent: 0
    };

    this.machine3 = {
      name: "Machine 3 (Consumer A)",
      ip: "192.168.1.30",
      jdkInstalled: true,
      kafkaInstalled: true,
      isConsuming: false,
      activeTopic: null,
      groupId: null,
      fromBeginning: false,
      consumedMessages: []
    };

    this.machine4 = {
      name: "Machine 4 (Consumer B)",
      ip: "192.168.1.40",
      jdkInstalled: true,
      kafkaInstalled: true,
      isConsuming: false,
      activeTopic: null,
      groupId: null,
      fromBeginning: false,
      consumedMessages: []
    };

    // Global Event Queue / Topic Storage
    this.topics = {};

    // Mission Steps Progress
    this.stepsCompleted = {
      checkIp: false,
      clusterId: false,
      storageFormat: false,
      brokerStart: false,
      topicCreate: false,
      producerStart: false,
      consumerStart: false,
      messageStreamed: false
    };

    this.eventListeners = [];
    this.history = {
      m1: [],
      m2: [],
      m3: [],
      m4: []
    };
  }

  // Subscribe to state updates (e.g. packet animation or UI refresh)
  on(event, callback) {
    this.eventListeners.push({ event, callback });
  }

  emit(event, data) {
    this.eventListeners
      .filter(l => l.event === event)
      .forEach(l => l.callback(data));
  }

  // Generate standard random UUID for Kafka KRaft Cluster
  generateClusterId() {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let result = "";
    for (let i = 0; i < 22; i++) {
      result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    this.machine1.clusterId = result;
    this.stepsCompleted.clusterId = true;
    this.emit("stepUpdated", "clusterId");
    return result;
  }

  // Format Storage
  formatStorage(clusterId) {
    if (!clusterId || clusterId.trim() === "") {
      return { success: false, error: "Missing required cluster ID: -t <CLUSTER_ID>" };
    }
    this.machine1.storageFormatted = true;
    this.machine1.clusterId = clusterId.trim();
    this.stepsCompleted.storageFormat = true;
    this.emit("stepUpdated", "storageFormat");
    return {
      success: true,
      output: `Formatting ${this.machine1.logDirs} with metadata.version 3.7-IV4 and cluster ID ${this.machine1.clusterId}`
    };
  }

  // Start Broker
  startBroker() {
    if (!this.machine1.storageFormatted) {
      return {
        success: false,
        error: "FATAL [KafkaServer id=1] Fatal error during KafkaServer startup. Log directory has not been formatted with kafka-storage.bat format!"
      };
    }

    this.machine1.isRunning = true;
    this.stepsCompleted.brokerStart = true;
    this.emit("stepUpdated", "brokerStart");
    this.emit("brokerStateChanged", true);

    return {
      success: true,
      logs: [
        `[${new Date().toISOString()}] INFO [KafkaServer id=1] starting (kafka.server.KafkaServer)`,
        `[${new Date().toISOString()}] INFO [KRaftController id=1] Initializing KRaft consensus metadata quorum`,
        `[${new Date().toISOString()}] INFO [SocketServer listenerType=ZK_BROKER, nodeId=1] Created socket server on ${this.machine1.listeners}`,
        `[${new Date().toISOString()}] INFO [MetadataLoader id=1] Metadata loader is now ready`,
        `[${new Date().toISOString()}] INFO [KafkaServer id=1] started with advertised.listeners = ${this.machine1.advertisedListeners}`
      ]
    };
  }

  // Stop Broker
  stopBroker() {
    this.machine1.isRunning = false;
    this.emit("brokerStateChanged", false);
    return { success: true, message: "Kafka Broker shut down successfully." };
  }

  // Create Topic
  createTopic(topicName, bootstrapServer, partitions = 1, repFactor = 1) {
    // Check network connectivity
    const netCheck = this.checkConnectivity(bootstrapServer);
    if (!netCheck.success) {
      return netCheck;
    }

    if (this.topics[topicName]) {
      return { success: false, error: `Topic '${topicName}' already exists in cluster.` };
    }

    this.topics[topicName] = {
      name: topicName,
      partitions: parseInt(partitions) || 1,
      replicationFactor: parseInt(repFactor) || 1,
      messages: []
    };

    this.stepsCompleted.topicCreate = true;
    this.emit("stepUpdated", "topicCreate");
    this.emit("topicCreated", topicName);

    return {
      success: true,
      output: `Created topic ${topicName} (partitions: ${partitions}, replication-factor: ${repFactor})`
    };
  }

  // Producer Start
  startProducer(topicName, bootstrapServer) {
    const netCheck = this.checkConnectivity(bootstrapServer);
    if (!netCheck.success) {
      return netCheck;
    }

    if (!this.topics[topicName]) {
      return {
        success: false,
        error: `Error: Topic '${topicName}' does not exist on broker '${bootstrapServer}'. Create it first!`
      };
    }

    this.machine2.isProducing = true;
    this.machine2.activeTopic = topicName;
    this.stepsCompleted.producerStart = true;
    this.emit("stepUpdated", "producerStart");

    return {
      success: true,
      prompt: `>`
    };
  }

  // Send Message from Producer
  sendMessage(messageText) {
    if (!this.machine2.isProducing) {
      return { success: false, error: "Producer is not active." };
    }

    if (!this.machine1.isRunning) {
      return {
        success: false,
        error: `[Producer clientId=console-producer] Connection to node -1 (${this.machine1.ip}:9092) could not be established. Broker may not be available.`
      };
    }

    // Check advertised listeners setting bug simulation
    if (this.machine1.advertisedListeners.includes("localhost")) {
      return {
        success: false,
        error: `[Producer clientId=console-producer] WARN: [Producer clientId=console-producer] Received metadata with advertised listener 'localhost:9092'. Machine 2 attempted connecting to its own localhost and timed out! Please set advertised.listeners=PLAINTEXT://${this.machine1.ip}:9092 on Machine 1.`
      };
    }

    // Check Firewall
    if (!this.machine1.firewallPort9092Allowed) {
      return {
        success: false,
        error: `[Producer clientId=console-producer] Connection refused: Machine 1 (${this.machine1.ip}:9092) is unreachable. Windows Firewall is blocking inbound port 9092.`
      };
    }

    const topic = this.topics[this.machine2.activeTopic];
    if (!topic) {
      return { success: false, error: "Active topic disappeared." };
    }

    const newOffset = topic.messages.length;
    const timestamp = new Date().toLocaleTimeString();
    const partition = newOffset % topic.partitions;
    const event = {
      offset: newOffset,
      key: `key-${newOffset}`,
      value: messageText,
      timestamp: timestamp,
      partition: partition
    };

    topic.messages.push(event);
    this.machine2.messagesSent++;
    this.machine1.totalCommittedMessages++;

    this.stepsCompleted.messageStreamed = true;
    this.emit("stepUpdated", "messageStreamed");

    // Trigger visual packet flow
    this.emit("packetFlow", {
      from: "machine2",
      to: "machine1",
      event: event,
      onDelivered: () => {
        // Now deliver to active consumers based on partition & group assignment
        let targetConsumers = [];
        let groupAssignments = {}; // groupId -> [machineIds]
        
        ['machine3', 'machine4'].forEach(mId => {
          const m = this[mId];
          if (m && m.isConsuming && m.activeTopic === this.machine2.activeTopic) {
             if (m.groupId) {
                if (!groupAssignments[m.groupId]) groupAssignments[m.groupId] = [];
                groupAssignments[m.groupId].push(mId);
             } else {
                targetConsumers.push(mId); // Independent consumers get all messages
             }
          }
        });
        
        // Resolve consumer group assignments (shard partitions across consumers in group)
        Object.keys(groupAssignments).forEach(gId => {
          const members = groupAssignments[gId].sort(); // e.g. ['machine3', 'machine4']
          const memberIndex = event.partition % members.length;
          targetConsumers.push(members[memberIndex]);
        });
        
        targetConsumers.forEach(consumerId => {
           this.emit("packetFlow", {
             from: "machine1",
             to: consumerId,
             event: event,
             onDelivered: () => {
               this[consumerId].consumedMessages.push(event);
               this.emit("messageConsumed", { consumerId, event });
             }
           });
        });
      }
    });

    this.emit("logUpdated", { topic: topic.name, message: event });

    return {
      success: true,
      event: event
    };
  }

  // Consumer Start
  startConsumer(topicName, bootstrapServer, fromBeginning = false, machineId = "machine3", groupId = null) {
    const netCheck = this.checkConnectivity(bootstrapServer);
    if (!netCheck.success) {
      return netCheck;
    }

    if (!this.topics[topicName]) {
      return {
        success: false,
        error: `Error: Topic '${topicName}' does not exist on broker.`
      };
    }

    const machine = this[machineId];
    if (!machine) {
      return { success: false, error: "Invalid consumer machine." };
    }

    machine.isConsuming = true;
    machine.activeTopic = topicName;
    machine.fromBeginning = fromBeginning;
    machine.groupId = groupId;
    this.stepsCompleted.consumerStart = true;
    this.emit("stepUpdated", "consumerStart");

    const existingMessages = fromBeginning ? [...this.topics[topicName].messages] : [];
    machine.consumedMessages = [...existingMessages];

    return {
      success: true,
      initialMessages: existingMessages
    };
  }

  // Network Connectivity Checker
  checkConnectivity(targetAddress) {
    if (!targetAddress) {
      return { success: false, error: "Missing --bootstrap-server argument." };
    }

    const cleanTarget = targetAddress.replace("PLAINTEXT://", "");
    const [targetHost, targetPort] = cleanTarget.split(":");

    if (targetPort !== "9092") {
      return {
        success: false,
        error: `Could not connect to ${targetAddress}: Port ${targetPort} is not Kafka broker standard plaintext port (9092).`
      };
    }

    if (targetHost === "localhost" || targetHost === "127.0.0.1") {
      // Trying to connect to localhost from machine 2 or 3
      return {
        success: false,
        error: `Connection refused: Remote machine attempted to connect to 'localhost:9092'. On a multi-machine setup, you must specify Machine 1's actual IP (${this.machine1.ip}:9092).`
      };
    }

    if (targetHost !== this.machine1.ip) {
      return {
        success: false,
        error: `Unknown host or unreachable IP address '${targetHost}'. Expected Machine 1 IP '${this.machine1.ip}'.`
      };
    }

    if (!this.machine1.firewallPort9092Allowed) {
      return {
        success: false,
        error: `Connection timed out to ${this.machine1.ip}:9092. Windows Firewall on Machine 1 is blocking Inbound TCP port 9092. Allow it with New-NetFirewallRule!`
      };
    }

    if (!this.machine1.isRunning) {
      return {
        success: false,
        error: `Connection refused: Kafka broker at ${this.machine1.ip}:9092 is not running. Start it on Machine 1 first with kafka-server-start.bat!`
      };
    }

    return { success: true };
  }

  // Toggle Firewall Simulation
  setFirewallRule(allowed) {
    this.machine1.firewallPort9092Allowed = allowed;
    this.emit("firewallToggled", allowed);
  }

  // Toggle Advertised Listener Mode (for Bug Simulation)
  setAdvertisedListenerMode(mode) {
    if (mode === "localhost") {
      this.machine1.advertisedListeners = "PLAINTEXT://localhost:9092";
    } else {
      this.machine1.advertisedListeners = `PLAINTEXT://${this.machine1.ip}:9092`;
    }
    this.emit("advertisedListenersChanged", this.machine1.advertisedListeners);
  }

  // Reset Simulator
  reset() {
    this.machine1.isRunning = false;
    this.machine1.storageFormatted = false;
    this.machine1.clusterId = null;
    this.machine1.totalCommittedMessages = 0;
    this.machine2.isProducing = false;
    this.machine2.messagesSent = 0;
    this.machine3.isConsuming = false;
    this.machine3.consumedMessages = [];
    this.topics = {};
    this.stepsCompleted = {
      checkIp: false,
      clusterId: false,
      storageFormat: false,
      brokerStart: false,
      topicCreate: false,
      producerStart: false,
      consumerStart: false,
      messageStreamed: false
    };
    this.emit("simulatorReset");
  }
}

// Export singleton instance for browser window
if (typeof window !== "undefined") {
  window.KafkaVirtualLab = KafkaVirtualLab;
}
