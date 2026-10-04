/**
 * Canvas Network Visualizer for Kafka 3-Machine Topology
 * Draws interactive nodes (Machine 1 Broker, Machine 2 Producer, Machine 3 Consumer, Virtual Switch)
 * and animates real-time packet transmissions.
 */

class NetworkVisualizer {
  constructor(canvasId) {
    this.canvas = document.getElementById(canvasId);
    if (!this.canvas) return;
    this.ctx = this.canvas.getContext("2d");
    this.packets = [];
    this.particles = [];
    this.nodes = {};
    this.animId = null;

    this.resize();
    window.addEventListener("resize", () => this.resize());
    this.initNodes();
    this.startLoop();
  }

  resize() {
    if (!this.canvas) return;
    const rect = this.canvas.getBoundingClientRect();
    this.canvas.width = rect.width * (window.devicePixelRatio || 1);
    this.canvas.height = rect.height * (window.devicePixelRatio || 1);
    this.ctx.scale(window.devicePixelRatio || 1, window.devicePixelRatio || 1);
    this.width = rect.width;
    this.height = rect.height;
    this.initNodes();
  }

  initNodes() {
    const w = this.width || 800;
    const h = this.height || 260;

    // Switch in center
    const switchPos = { x: w * 0.5, y: h * 0.55 };

    // Machine 1 (Broker) on Top Center
    const m1Pos = { x: w * 0.5, y: h * 0.2 };

    // Machine 2 (Producer) on Bottom Left
    const m2Pos = { x: w * 0.2, y: h * 0.78 };

    // Machine 3 (Consumer) on Bottom Right
    const m3Pos = { x: w * 0.8, y: h * 0.78 };

    this.nodes = {
      switch: { x: switchPos.x, y: switchPos.y, label: "Virtual Switch (LAN)", color: "#64748b", icon: "hub" },
      machine1: { x: m1Pos.x, y: m1Pos.y, label: "Machine 1: Broker", ip: "192.168.1.10:9092", color: "#4f46e5", role: "Broker" },
      machine2: { x: m2Pos.x, y: m2Pos.y, label: "Machine 2: Producer", ip: "192.168.1.20", color: "#059669", role: "Producer" },
      machine3: { x: m3Pos.x, y: m3Pos.y, label: "Machine 3: Consumer", ip: "192.168.1.30", color: "#d97706", role: "Consumer" }
    };
  }

  // Spawn an animated packet moving from source to target node
  spawnPacket(fromKey, toKey, data, onComplete) {
    const fromNode = this.nodes[fromKey];
    const toNode = this.nodes[toKey];
    const switchNode = this.nodes.switch;

    if (!fromNode || !toNode || !switchNode) return;

    // Multi-segment path: fromNode -> switch -> toNode
    const path = [
      { x: fromNode.x, y: fromNode.y },
      { x: switchNode.x, y: switchNode.y },
      { x: toNode.x, y: toNode.y }
    ];

    this.packets.push({
      path: path,
      currentSegment: 0,
      progress: 0,
      speed: 0.025,
      data: data,
      color: fromKey === "machine2" ? "#10b981" : "#4f46e5",
      onComplete: onComplete
    });
  }

  update() {
    // Update Packets
    for (let i = this.packets.length - 1; i >= 0; i--) {
      const p = this.packets[i];
      p.progress += p.speed;

      // Spawn trail particle
      const segStart = p.path[p.currentSegment];
      const segEnd = p.path[p.currentSegment + 1];
      const currentX = segStart.x + (segEnd.x - segStart.x) * p.progress;
      const currentY = segStart.y + (segEnd.y - segStart.y) * p.progress;

      if (Math.random() < 0.4) {
        this.particles.push({
          x: currentX + (Math.random() - 0.5) * 6,
          y: currentY + (Math.random() - 0.5) * 6,
          radius: Math.random() * 2 + 1,
          alpha: 1,
          color: p.color
        });
      }

      if (p.progress >= 1) {
        p.progress = 0;
        p.currentSegment++;
        if (p.currentSegment >= p.path.length - 1) {
          // Packet finished journey
          if (p.onComplete) p.onComplete();
          this.packets.splice(i, 1);
        }
      }
    }

    // Update Particles
    for (let i = this.particles.length - 1; i >= 0; i--) {
      const pt = this.particles[i];
      pt.alpha -= 0.03;
      if (pt.alpha <= 0) {
        this.particles.splice(i, 1);
      }
    }
  }

  draw() {
    if (!this.ctx) return;
    const ctx = this.ctx;
    const isDark = document.documentElement.classList.contains("dark") || true;

    ctx.clearRect(0, 0, this.width, this.height);

    // Draw connecting cables
    const switchNode = this.nodes.switch;
    const cableColor = isDark ? "#334155" : "#cbd5e1";

    ["machine1", "machine2", "machine3"].forEach(key => {
      const node = this.nodes[key];
      if (!node) return;

      ctx.beginPath();
      ctx.moveTo(node.x, node.y);
      ctx.lineTo(switchNode.x, switchNode.y);
      ctx.strokeStyle = cableColor;
      ctx.lineWidth = 2.5;
      ctx.setLineDash([4, 4]);
      ctx.stroke();
      ctx.setLineDash([]);
    });

    // Draw Particles
    this.particles.forEach(pt => {
      ctx.beginPath();
      ctx.arc(pt.x, pt.y, pt.radius, 0, Math.PI * 2);
      ctx.fillStyle = pt.color;
      ctx.globalAlpha = Math.max(0, pt.alpha);
      ctx.fill();
      ctx.globalAlpha = 1;
    });

    // Draw Packets
    this.packets.forEach(p => {
      const segStart = p.path[p.currentSegment];
      const segEnd = p.path[p.currentSegment + 1];
      const x = segStart.x + (segEnd.x - segStart.x) * p.progress;
      const y = segStart.y + (segEnd.y - segStart.y) * p.progress;

      // Glow effect
      ctx.shadowColor = p.color;
      ctx.shadowBlur = 12;
      ctx.beginPath();
      ctx.arc(x, y, 7, 0, Math.PI * 2);
      ctx.fillStyle = p.color;
      ctx.fill();

      // Inner white core
      ctx.beginPath();
      ctx.arc(x, y, 3, 0, Math.PI * 2);
      ctx.fillStyle = "#ffffff";
      ctx.fill();

      // Reset shadow
      ctx.shadowBlur = 0;
    });

    // Draw Switch in Center
    ctx.beginPath();
    ctx.arc(switchNode.x, switchNode.y, 16, 0, Math.PI * 2);
    ctx.fillStyle = "#1e293b";
    ctx.strokeStyle = "#475569";
    ctx.lineWidth = 2;
    ctx.fill();
    ctx.stroke();

    ctx.fillStyle = "#94a3b8";
    ctx.font = "bold 10px Roboto, sans-serif";
    ctx.textAlign = "center";
    ctx.fillText("LAN SWITCH", switchNode.x, switchNode.y + 28);

    // Draw Machine Nodes
    ["machine1", "machine2", "machine3"].forEach(key => {
      const node = this.nodes[key];
      if (!node) return;

      // Box
      const boxW = 150;
      const boxH = 54;
      const boxX = node.x - boxW / 2;
      const boxY = node.y - boxH / 2;

      // Card Background
      ctx.fillStyle = "#0f172a";
      ctx.strokeStyle = node.color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.roundRect(boxX, boxY, boxW, boxH, 8);
      ctx.fill();
      ctx.stroke();

      // Status indicator light
      ctx.beginPath();
      ctx.arc(boxX + 16, boxY + 18, 5, 0, Math.PI * 2);
      ctx.fillStyle = node.color;
      ctx.shadowColor = node.color;
      ctx.shadowBlur = 8;
      ctx.fill();
      ctx.shadowBlur = 0;

      // Node Name
      ctx.fillStyle = "#f8fafc";
      ctx.font = "bold 11px Inter, sans-serif";
      ctx.textAlign = "left";
      ctx.fillText(node.label, boxX + 28, boxY + 22);

      // Node IP
      ctx.fillStyle = "#94a3b8";
      ctx.font = "10px Fira Code, monospace";
      ctx.fillText(node.ip, boxX + 28, boxY + 38);
    });
  }

  startLoop() {
    const loop = () => {
      this.update();
      this.draw();
      this.animId = requestAnimationFrame(loop);
    };
    this.animId = requestAnimationFrame(loop);
  }

  destroy() {
    if (this.animId) {
      cancelAnimationFrame(this.animId);
    }
  }
}

if (typeof window !== "undefined") {
  window.NetworkVisualizer = NetworkVisualizer;
}
