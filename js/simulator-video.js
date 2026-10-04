class KafkaVideoLab {
  constructor() {
    this.broker = {
      isRunning: false,
      ip: "192.168.1.10",
      port: "9092"
    };

    this.topic = {
      name: "video-stream",
      messages: []
    };

    this.eventListeners = [];
    
    this.stepsCompleted = {
      brokerStart: false,
      producerRun: false,
      consumerRun: false,
      customTask: false
    };

    this.currentSleepTime = 0.1;

    this.isStreaming = false;
    this.isConsuming = false;
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

  // Strict (Frontend-only) Python Syntax Validator
  validatePython(code) {
    const lines = code.split('\n');
    let openBrackets = 0;
    
    for (let i = 0; i < lines.length; i++) {
      let line = lines[i];
      let trimmed = line.trim();
      
      if (trimmed === "" || trimmed.startsWith("#")) continue;

      openBrackets += (line.match(/\(/g) || []).length;
      openBrackets += (line.match(/\[/g) || []).length;
      openBrackets += (line.match(/\{/g) || []).length;
      openBrackets -= (line.match(/\)/g) || []).length;
      openBrackets -= (line.match(/\]/g) || []).length;
      openBrackets -= (line.match(/\}/g) || []).length;

      if (trimmed.startsWith("if ") && !trimmed.endsWith(":")) return { valid: false, error: `SyntaxError: expected ':' (line ${i+1})` };
      if (trimmed.startsWith("for ") && !trimmed.endsWith(":")) return { valid: false, error: `SyntaxError: expected ':' (line ${i+1})` };
      if (trimmed.startsWith("while ") && !trimmed.endsWith(":")) return { valid: false, error: `SyntaxError: expected ':' (line ${i+1})` };

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

    const syntaxCheck = this.validatePython(code);
    if (!syntaxCheck.valid) {
      return { success: false, error: syntaxCheck.error };
    }

    const cleanCode = code.replace(/\s/g, ''); 

    // PRODUCER LOGIC
    if (type === 'producer') {
      if (!code.includes("import cv2")) {
        return { success: false, error: "NameError: name 'cv2' is not defined. Did you import OpenCV?" };
      }
      if (!code.includes("from kafka import KafkaProducer")) {
        return { success: false, error: "NameError: name 'KafkaProducer' is not defined." };
      }
      if (!cleanCode.includes("cv2.VideoCapture(")) {
        return { success: false, error: "LogicError: You must initialize cv2.VideoCapture() to capture frames." };
      }
      if (!cleanCode.includes("producer.send(")) {
         return { success: false, error: "LogicError: You didn't call producer.send() to stream frames!" };
      }
      if (!cleanCode.includes("cv2.imencode")) {
         return { success: false, error: "LogicError: You must encode frames to JPEG bytes using cv2.imencode before sending." };
      }

      let sleepTime = 0.1;
      const sleepMatch = code.match(/time\.sleep\(\s*([\d\.]+)\s*\)/);
      if (sleepMatch) {
         sleepTime = parseFloat(sleepMatch[1]);
      }
      this.currentSleepTime = sleepTime;

      if (sleepTime === 0.5) {
         this.stepsCompleted.customTask = true;
         this.emit("stepUpdated", "customTask");
      }

      this.stepsCompleted.producerRun = true;
      this.isStreaming = true;
      this.emit("stepUpdated", "producerRun");
      this.emit("producerStarted", true);

      return { 
        success: true, 
        output: `> OpenCV Producer Initialized.\n> Capturing webcam frames...\n> Streaming encoded frames to topic '${this.topic.name}'...` 
      };
    }

    // CONSUMER LOGIC
    if (type === 'consumer') {
      if (!code.includes("import cv2")) {
        return { success: false, error: "NameError: name 'cv2' is not defined. Did you import OpenCV?" };
      }
      if (!code.includes("from kafka import KafkaConsumer")) {
        return { success: false, error: "NameError: name 'KafkaConsumer' is not defined." };
      }
      if (!code.includes("for") || !code.includes("in consumer:")) {
         return { success: false, error: "LogicError: You need a 'for message in consumer:' loop to read stream." };
      }

      this.stepsCompleted.consumerRun = true;
      this.isConsuming = true;
      this.emit("stepUpdated", "consumerRun");
      this.emit("consumerStarted", true);

      return { 
        success: true, 
        output: `> OpenCV Consumer Initialized.\n> Connected to topic '${this.topic.name}'.\n> Awaiting video frames...`
      };
    }

    return { success: false, error: "Unknown execution type." };
  }
}

if (typeof window !== "undefined") {
  window.KafkaVideoLab = KafkaVideoLab;
}
