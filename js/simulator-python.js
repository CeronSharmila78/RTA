class KafkaPythonLab {
  constructor() {
    this.broker = {
      isRunning: false,
      ip: "192.168.1.10",
      port: "9092"
    };

    this.topic = {
      name: "fsd-topic",
      messages: []
    };

    this.eventListeners = [];
    
    this.stepsCompleted = {
      brokerStart: false,
      producerRun: false,
      consumerRun: false,
      customTask: false
    };
  }

  on(event, callback) {
    this.eventListeners.push({ event, callback });
  }

  emit(event, data) {
    this.eventListeners.filter(l => l.event === event).forEach(l => l.callback(data));
  }

  startBroker() {
    this.broker.isRunning = true;
    this.stepsCompleted.brokerStart = true;
    this.emit("stepUpdated", "brokerStart");
    return {
      success: true,
      logs: [
        "[KafkaServer id=1] starting (kafka.server.KafkaServer)",
        `[SocketServer] Created socket server on PLAINTEXT://${this.broker.ip}:${this.broker.port}`,
        "[KafkaServer id=1] STARTED."
      ]
    };
  }

  stopBroker() {
    this.broker.isRunning = false;
    return { success: true };
  }

  // Strict (Frontend-only) Python Syntax Validator
  validatePython(code) {
    const lines = code.split('\n');
    let openBrackets = 0;
    
    for (let i = 0; i < lines.length; i++) {
      let line = lines[i];
      let trimmed = line.trim();
      
      if (trimmed === "" || trimmed.startsWith("#")) continue;

      // Check matching brackets (simple heuristic)
      openBrackets += (line.match(/\(/g) || []).length;
      openBrackets += (line.match(/\[/g) || []).length;
      openBrackets += (line.match(/\{/g) || []).length;
      openBrackets -= (line.match(/\)/g) || []).length;
      openBrackets -= (line.match(/\]/g) || []).length;
      openBrackets -= (line.match(/\}/g) || []).length;

      // Strict check: if statement missing colon
      if (trimmed.startsWith("if ") && !trimmed.endsWith(":")) {
        return { valid: false, error: `SyntaxError: expected ':' (line ${i+1})` };
      }
      
      // Strict check: for loop missing colon
      if (trimmed.startsWith("for ") && !trimmed.endsWith(":")) {
        return { valid: false, error: `SyntaxError: expected ':' (line ${i+1})` };
      }
      
      // Strict check: while loop missing colon
      if (trimmed.startsWith("while ") && !trimmed.endsWith(":")) {
        return { valid: false, error: `SyntaxError: expected ':' (line ${i+1})` };
      }

      // Check for mismatched quotes on the line (very basic)
      const singleQuotes = (line.match(/'/g) || []).length;
      const doubleQuotes = (line.match(/"/g) || []).length;
      if (singleQuotes % 2 !== 0 || doubleQuotes % 2 !== 0) {
        return { valid: false, error: `SyntaxError: unterminated string literal (line ${i+1})` };
      }
    }

    if (openBrackets !== 0) {
      return { valid: false, error: `SyntaxError: unexpected EOF while parsing (mismatched brackets)` };
    }

    return { valid: true };
  }

  runCode(code, type) {
    if (!this.broker.isRunning) {
      return { success: false, error: `kafka.errors.NoBrokersAvailable: NoBrokersAvailable` };
    }

    // 1. Strict Syntax Check
    const syntaxCheck = this.validatePython(code);
    if (!syntaxCheck.valid) {
      return { success: false, error: syntaxCheck.error };
    }

    const cleanCode = code.replace(/\s/g, ''); // Remove all whitespace for easy keyword searching

    // 2. Logic simulation for PRODUCER
    if (type === 'producer') {
      if (!code.includes("from kafka import KafkaProducer")) {
        return { success: false, error: "NameError: name 'KafkaProducer' is not defined. Did you import it?" };
      }
      
      if (!cleanCode.includes(`bootstrap_servers=['${this.broker.ip}:${this.broker.port}']`) && 
          !cleanCode.includes(`bootstrap_servers=["${this.broker.ip}:${this.broker.port}"]`)) {
        return { success: false, error: `kafka.errors.NodeNotReadyError: Could not connect to broker. Check your bootstrap_servers configuration.` };
      }

      if (!cleanCode.includes("producer.send(")) {
         return { success: false, error: "LogicError: You didn't call producer.send() to send any messages!" };
      }

      // Simulate sending messages
      let sentCount = (code.match(/producer\.send\(/g) || []).length;
      
      // Extract number from range() if a loop is used
      const rangeMatch = code.match(/range\(\s*(\d+)\s*\)/);
      if (rangeMatch) {
        sentCount = parseInt(rangeMatch[1], 10);
      } else if (code.includes("while ")) {
        sentCount = 5; 
      }

      if (sentCount === 15) {
         this.stepsCompleted.customTask = true;
         this.emit("stepUpdated", "customTask");
      }

      let isJson = code.includes("json.dumps");

      for (let i = 0; i < sentCount; i++) {
        let msg = `Programmatic message ${this.topic.messages.length + 1}`;
        if (isJson) {
           msg = `{"sensor": "S1", "temp": ${Math.floor(Math.random() * (45 - 20 + 1)) + 20}}`;
        }
        const event = { value: msg, offset: this.topic.messages.length };
        this.topic.messages.push(event);
        this.emit("packetFlow", { from: "python_producer", to: "broker", event });
      }

      this.stepsCompleted.producerRun = true;
      this.emit("stepUpdated", "producerRun");

      return { 
        success: true, 
        output: `> Python Script Executed Successfully.\n> Sent ${sentCount} message(s) to topic '${this.topic.name}'.` 
      };
    }

    // 3. Logic simulation for CONSUMER
    if (type === 'consumer') {
      if (!code.includes("from kafka import KafkaConsumer")) {
        return { success: false, error: "NameError: name 'KafkaConsumer' is not defined. Did you import it?" };
      }

      if (!cleanCode.includes(`bootstrap_servers=['${this.broker.ip}:${this.broker.port}']`) && 
          !cleanCode.includes(`bootstrap_servers=["${this.broker.ip}:${this.broker.port}"]`)) {
        return { success: false, error: `kafka.errors.NodeNotReadyError: Could not connect to broker. Check your bootstrap_servers configuration.` };
      }

      if (!code.includes("for") || !code.includes("in consumer:")) {
         return { success: false, error: "LogicError: You need a 'for message in consumer:' loop to read messages." };
      }

      this.stepsCompleted.consumerRun = true;
      this.emit("stepUpdated", "consumerRun");

      let outputLogs = "> Python Script Executed Successfully.\n> Listening for messages...\n";
      
      if (this.topic.messages.length === 0) {
        outputLogs += "> (No messages found in topic yet. Run producer first!)";
      } else {
        let delayCount = 0;
        this.topic.messages.forEach(m => {
          outputLogs += `ConsumerRecord(topic='${this.topic.name}', partition=0, offset=${m.offset}, value=b'${m.value}')\n`;
          
          // Emulate parsing JSON telemetry for the chart
          try {
             if (m.value.includes("{")) {
                const parsed = JSON.parse(m.value);
                if (parsed && typeof parsed.temp !== 'undefined') {
                   // Emit with slight delay to look like live streaming
                   setTimeout(() => {
                       this.emit('telemetryReceived', parsed);
                   }, delayCount * 250);
                   delayCount++;
                }
             }
          } catch(e) {}
        });
      }

      return { 
        success: true, 
        output: outputLogs
      };
    }

    return { success: false, error: "Unknown execution type." };
  }
}

if (typeof window !== "undefined") {
  window.KafkaPythonLab = KafkaPythonLab;
}
