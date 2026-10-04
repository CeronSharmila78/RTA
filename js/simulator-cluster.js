class KafkaClusterLab {
  constructor() {
    this.brokers = {
      1: { id: 1, isRunning: false, ip: '192.168.1.11', name: 'Broker 1' },
      2: { id: 2, isRunning: false, ip: '192.168.1.12', name: 'Broker 2' },
      3: { id: 3, isRunning: false, ip: '192.168.1.13', name: 'Broker 3' }
    };
    
    this.client = {
      id: 4,
      ip: '192.168.1.99',
      name: 'Client',
      activeTopic: null,
      isProducing: false,
      isConsuming: false,
      consumedMessages: []
    };

    this.topics = {};
    
    this.eventListeners = [];
    
    // Milestones tracking
    this.stepsCompleted = {
      allBrokersUp: false,
      topicCreated: false,
      messagesProduced: false,
      leaderCrashed: false,
      electionTriggered: false
    };
  }

  on(event, callback) {
    this.eventListeners.push({ event, callback });
  }

  emit(event, data) {
    this.eventListeners.filter(l => l.event === event).forEach(l => l.callback(data));
  }

  startBroker(id) {
    if (!this.brokers[id]) return { success: false, error: "Invalid broker ID" };
    if (this.brokers[id].isRunning) return { success: false, error: "Broker already running" };
    
    this.brokers[id].isRunning = true;
    this.emit("brokerStateChanged", { id, state: "online" });
    
    const allUp = [1,2,3].every(b => this.brokers[b].isRunning);
    if (allUp && !this.stepsCompleted.allBrokersUp) {
      this.stepsCompleted.allBrokersUp = true;
      this.emit("stepUpdated", "allBrokersUp");
    }

    // Attempt to rejoin as replica for existing partitions
    Object.keys(this.topics).forEach(topicName => {
      const topic = this.topics[topicName];
      topic.partitions.forEach(p => {
        if (p.replicas.includes(id) && !p.isr.includes(id)) {
          p.isr.push(id);
          this.emit("isrUpdated", { topic: topicName, partition: p.id, isr: p.isr });
        }
      });
    });

    return { success: true };
  }

  crashBroker(id) {
    if (!this.brokers[id]) return { success: false, error: "Invalid broker ID" };
    if (!this.brokers[id].isRunning) return { success: false, error: "Broker already offline" };
    
    this.brokers[id].isRunning = false;
    this.emit("brokerStateChanged", { id, state: "offline" });
    
    // Check if this broker was leader for any partition
    Object.keys(this.topics).forEach(topicName => {
      const topic = this.topics[topicName];
      topic.partitions.forEach(p => {
        // Remove from ISR
        p.isr = p.isr.filter(rid => rid !== id);
        
        if (p.leader === id) {
          if (!this.stepsCompleted.leaderCrashed) {
             this.stepsCompleted.leaderCrashed = true;
             this.emit("stepUpdated", "leaderCrashed");
          }
          
          this.emit("logUpdated", { brokerId: id, msg: `[Broker ${id}] FATAL EXCEPTION: Process exited unexpectedly.` });
          this.emit("electionTriggered", { topic: topicName, partition: p.id, oldLeader: id });
          
          // Elect new leader from ISR
          if (p.isr.length > 0) {
            // Elect the first available ISR
            p.leader = p.isr[0];
            this.emit("leaderElected", { topic: topicName, partition: p.id, newLeader: p.leader });
            
            if (!this.stepsCompleted.electionTriggered) {
              this.stepsCompleted.electionTriggered = true;
              this.emit("stepUpdated", "electionTriggered");
            }
          } else {
            p.leader = null;
            this.emit("leaderFailed", { topic: topicName, partition: p.id, error: "No in-sync replicas available for election!" });
          }
        }
        this.emit("isrUpdated", { topic: topicName, partition: p.id, isr: p.isr });
      });
    });

    return { success: true };
  }

  createTopic(topicName, partitionsCount = 1, replicationFactor = 3) {
    if (this.topics[topicName]) {
      return { success: false, error: `Topic '${topicName}' already exists.` };
    }
    
    const activeBrokers = [1,2,3].filter(id => this.brokers[id].isRunning);
    if (activeBrokers.length < replicationFactor) {
      return { success: false, error: `Not enough active brokers to satisfy replication factor ${replicationFactor}.` };
    }

    const partitions = [];
    for (let i = 0; i < partitionsCount; i++) {
      // Round robin replica assignment
      const replicas = [];
      for (let r = 0; r < replicationFactor; r++) {
        replicas.push(activeBrokers[(i + r) % activeBrokers.length]);
      }
      
      partitions.push({
        id: i,
        leader: replicas[0],
        replicas: replicas,
        isr: [...replicas], // All replicas start in-sync
        messages: []
      });
    }

    this.topics[topicName] = {
      name: topicName,
      partitions: partitions
    };

    if (!this.stepsCompleted.topicCreated) {
      this.stepsCompleted.topicCreated = true;
      this.emit("stepUpdated", "topicCreated");
    }

    // Trigger visual update for leaders
    partitions.forEach(p => {
      this.emit("leaderElected", { topic: topicName, partition: p.id, newLeader: p.leader });
    });

    return { success: true, output: `Created topic ${topicName}. Partitions: ${partitionsCount}, ReplicationFactor: ${replicationFactor}` };
  }

  startProducer(topicName) {
    if (!this.topics[topicName]) {
      return { success: false, error: `Topic '${topicName}' does not exist.` };
    }
    this.client.isProducing = true;
    this.client.activeTopic = topicName;
    return { success: true };
  }

  startConsumer(topicName) {
    if (!this.topics[topicName]) {
      return { success: false, error: `Topic '${topicName}' does not exist.` };
    }
    this.client.isConsuming = true;
    this.client.activeTopic = topicName;
    return { success: true };
  }

  sendMessage(messageText) {
    if (!this.client.isProducing || !this.client.activeTopic) {
      return { success: false, error: "Producer not running." };
    }

    const topic = this.topics[this.client.activeTopic];
    if (!topic) return { success: false, error: "Topic not found." };

    // Choose partition (round robin for simplicity)
    const partitionId = Math.floor(Math.random() * topic.partitions.length);
    const partition = topic.partitions[partitionId];
    
    if (!partition.leader) {
      return { success: false, error: `LeaderNotAvailableException: No leader for partition ${partitionId}` };
    }

    const event = {
      value: messageText,
      timestamp: Date.now(),
      partition: partitionId
    };

    partition.messages.push(event);

    // Simulate flow: Client -> Leader Broker -> Consumer (if active)
    this.emit("packetFlow", {
      from: "client",
      to: partition.leader,
      event: event,
      onDelivered: () => {
        // Log on the leader broker
        this.emit("logUpdated", { brokerId: partition.leader, msg: `[Leader P${partitionId}] Appended: ${messageText}` });
        
        // Replicate to ISRs
        partition.isr.filter(id => id !== partition.leader).forEach(followerId => {
          this.emit("packetFlow", {
            from: partition.leader,
            to: followerId,
            event: event,
            onDelivered: () => {
               this.emit("logUpdated", { brokerId: followerId, msg: `[Replica P${partitionId}] Synced: ${messageText}` });
            }
          });
        });

        // Send to consumer
        if (this.client.isConsuming && this.client.activeTopic === topic.name) {
          this.emit("packetFlow", {
            from: partition.leader,
            to: "client",
            event: event,
            onDelivered: () => {
              this.client.consumedMessages.push(event);
              this.emit("messageConsumed", event);
            }
          });
        }
      }
    });

    if (!this.stepsCompleted.messagesProduced) {
      this.stepsCompleted.messagesProduced = true;
      this.emit("stepUpdated", "messagesProduced");
    }

    return { success: true, event };
  }
}
