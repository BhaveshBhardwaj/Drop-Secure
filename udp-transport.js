const dgram = require('dgram');
const fs = require('fs');
const crypto = require('crypto');
const EventEmitter = require('events');

// Packet Types
const PKT_DATA = 1;
const PKT_ACK = 2;
const PKT_NACK = 3; // Optional direct negative ACK
const PKT_META = 4;
const PKT_META_ACK = 5;
const PKT_FIN = 6;
const PKT_FIN_ACK = 7;

class UdpSender extends EventEmitter {
  constructor(options = {}) {
    super();
    this.targetIp = options.targetIp || '127.0.0.1';
    this.targetPort = options.targetPort || 5001;
    this.localPort = options.localPort || 5000;
    this.blockSize = options.blockSize || 1400; // Fits nicely inside standard 1500-byte MTU
    this.windowSize = options.windowSize || 128; // Max outstanding packets
    this.speedLimit = options.speedLimit || 0; // Bytes/sec. 0 means unlimited.
    
    this.socket = null;
    this.fileFd = null;
    this.filePath = '';
    this.fileName = '';
    this.fileSize = 0;
    this.fileHash = '';
    this.totalChunks = 0;
    this.sessionId = Math.floor(Math.random() * 1000000);

    // Flow & Congestion Control / Reliability
    this.windowStart = 0; // Sequence number of the left edge of the sliding window
    this.nextSeqNum = 0;  // Next sequence number to be sent
    this.packets = new Map(); // seq -> { buffer, sentTime, retransmitCount, acked }
    
    // RTT / RTO Estimation
    this.srtt = 100; // Smooth RTT in ms
    this.rttvar = 25; // RTT variation
    this.rto = 150;  // Retransmission Timeout in ms
    
    // Rate Limiting (Token Bucket)
    this.tokens = 1024 * 1024; // Initial token bucket size (1MB)
    this.maxTokens = 1024 * 1024;
    this.lastTokenRefillTime = Date.now();

    // Stats
    this.stats = {
      bytesSent: 0,
      packetsSent: 0,
      packetsRetransmitted: 0,
      acksReceived: 0,
      currentSpeed: 0, // bytes per second
      rtt: 0,
      rto: 150,
      progress: 0,
      windowStart: 0,
      nextSeqNum: 0,
      status: 'idle' // idle, negotiating, transferring, completed, failed
    };

    this.timerId = null;
    this.speedIntervalId = null;
    this.isTransferring = false;
  }

  // Helper to create binary packet
  createPacket(type, seqNum, payloadBuffer) {
    const payloadLen = payloadBuffer ? payloadBuffer.length : 0;
    const header = Buffer.alloc(11);
    header.writeUInt8(type, 0);
    header.writeUInt32BE(seqNum, 1);
    header.writeUInt32BE(this.sessionId, 5);
    header.writeUInt16BE(payloadLen, 9);
    
    if (payloadBuffer) {
      return Buffer.concat([header, payloadBuffer]);
    }
    return header;
  }

  // Parse binary packet
  parsePacket(buffer) {
    if (buffer.length < 11) return null;
    const type = buffer.readUInt8(0);
    const seqNum = buffer.readUInt32BE(1);
    const sessionId = buffer.readUInt32BE(5);
    const length = buffer.readUInt16BE(9);
    const payload = buffer.slice(11, 11 + length);
    return { type, seqNum, sessionId, length, payload };
  }

  updateTokens() {
    if (this.speedLimit <= 0) {
      this.tokens = this.maxTokens;
      return;
    }
    const now = Date.now();
    const elapsed = now - this.lastTokenRefillTime;
    if (elapsed > 0) {
      const newTokens = elapsed * (this.speedLimit / 1000);
      this.tokens = Math.min(this.maxTokens, this.tokens + newTokens);
      this.lastTokenRefillTime = now;
    }
  }

  async startTransfer(filePath) {
    if (this.isTransferring) return;
    this.filePath = filePath;
    this.isTransferring = true;
    this.stats.status = 'negotiating';
    this.emit('stats', this.stats);

    try {
      const stats = fs.statSync(filePath);
      this.fileSize = stats.size;
      this.fileName = filePath.split(/[/\\]/).pop();
      this.totalChunks = Math.ceil(this.fileSize / this.blockSize);
      
      // Calculate file hash asynchronously
      this.emit('log', 'Calculating file hash...');
      this.fileHash = await this.calculateHash(filePath);
      this.emit('log', `File Hash: ${this.fileHash}`);

      this.fileFd = fs.openSync(filePath, 'r');
      
      // Initialize Socket
      this.socket = dgram.createSocket('udp4');
      this.socket.on('message', (msg) => this.handleMessage(msg));
      this.socket.on('error', (err) => {
        this.emit('error', err);
        this.cleanup(false, 'Socket error: ' + err.message);
      });

      this.socket.bind(this.localPort, () => {
        this.emit('log', `Sender UDP socket bound to port ${this.localPort}`);
        this.negotiateMetadata();
      });

      // Start speed reporting timer
      let lastBytes = 0;
      this.speedIntervalId = setInterval(() => {
        const delta = this.stats.bytesSent - lastBytes;
        this.stats.currentSpeed = delta; // bytes per second
        lastBytes = this.stats.bytesSent;
        this.stats.rtt = this.srtt;
        this.stats.rto = this.rto;
        this.stats.progress = this.totalChunks > 0 ? (this.windowStart / this.totalChunks) * 100 : 0;
        this.stats.windowStart = this.windowStart;
        this.stats.nextSeqNum = this.nextSeqNum;
        this.emit('stats', this.stats);
      }, 1000);

    } catch (err) {
      this.emit('error', err);
      this.cleanup(false, 'Initialization failed: ' + err.message);
    }
  }

  calculateHash(filePath) {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha256');
      const stream = fs.createReadStream(filePath);
      stream.on('data', (data) => hash.update(data));
      stream.on('end', () => resolve(hash.digest('hex')));
      stream.on('error', (err) => reject(err));
    });
  }

  negotiateMetadata() {
    const metaData = {
      filename: this.fileName,
      filesize: this.fileSize,
      totalChunks: this.totalChunks,
      blockSize: this.blockSize,
      hash: this.fileHash
    };
    
    const payload = Buffer.from(JSON.stringify(metaData));
    const packet = this.createPacket(PKT_META, 0, payload);

    const sendMeta = () => {
      if (this.stats.status !== 'negotiating') return;
      this.emit('log', `Sending file metadata (session ${this.sessionId}), waiting for ACK...`);
      this.socket.send(packet, 0, packet.length, this.targetPort, this.targetIp);
      
      // Resend after RTO if not acknowledged
      this.timerId = setTimeout(sendMeta, this.rto);
    };

    sendMeta();
  }

  handleMessage(msg) {
    const packet = this.parsePacket(msg);
    if (!packet || packet.sessionId !== this.sessionId) return;

    if (packet.type === PKT_META_ACK) {
      if (this.stats.status === 'negotiating') {
        clearTimeout(this.timerId);
        this.emit('log', 'Metadata acknowledged by receiver. Starting file data transfer.');
        this.stats.status = 'transferring';
        this.emit('stats', this.stats);
        
        // Start sliding window sending loop
        this.lastTokenRefillTime = Date.now();
        this.sendLoop();
        
        // Start retransmission check timer
        this.startRetransmitTimer();
      }
    } else if (packet.type === PKT_ACK) {
      this.stats.acksReceived++;
      try {
        const ackData = JSON.parse(packet.payload.toString());
        const cumulativeSeq = ackData.cumulativeSeq;
        const sackList = ackData.sack || [];

        // RTT estimation based on cumulative ACK or SACKed packets
        const ackedSeq = packet.seqNum; // We set seqNum of ACK to the packet it acknowledges
        const packetInfo = this.packets.get(ackedSeq);
        if (packetInfo && !packetInfo.acked && packetInfo.retransmitCount === 0) {
          const rtt = Date.now() - packetInfo.sentTime;
          this.updateRto(rtt);
        }

        // Handle Cumulative ACK
        if (cumulativeSeq > this.windowStart) {
          // Clean up all packets below the cumulative ACK
          for (let i = this.windowStart; i < cumulativeSeq; i++) {
            this.packets.delete(i);
          }
          this.windowStart = cumulativeSeq;
        }

        // Handle SACK (Selective ACKs)
        for (const seq of sackList) {
          const info = this.packets.get(seq);
          if (info) {
            info.acked = true;
          }
        }

        // Check if finished
        if (this.windowStart >= this.totalChunks) {
          this.sendFin();
        } else {
          // Wake up loop to send new window space
          this.sendLoop();
        }
      } catch (err) {
        this.emit('log', 'Error parsing ACK data: ' + err.message);
      }
    } else if (packet.type === PKT_FIN_ACK) {
      this.emit('log', 'Transfer complete! Receiver acknowledged file finalization.');
      this.cleanup(true);
    }
  }

  updateRto(rtt) {
    // EWMA filter for RTT/RTO
    this.srtt = 0.875 * this.srtt + 0.125 * rtt;
    this.rttvar = 0.75 * this.rttvar + 0.25 * Math.abs(this.srtt - rtt);
    this.rto = Math.max(50, Math.min(2000, Math.round(this.srtt + 4 * this.rttvar)));
  }

  sendFin() {
    if (this.stats.status !== 'transferring') return;
    this.stats.status = 'completing';
    
    const packet = this.createPacket(PKT_FIN, this.totalChunks, null);
    const sendFinPacket = () => {
      if (this.stats.status !== 'completing') return;
      this.emit('log', 'Sending FIN packet, waiting for verification ACK...');
      this.socket.send(packet, 0, packet.length, this.targetPort, this.targetIp);
      this.timerId = setTimeout(sendFinPacket, this.rto);
    };
    
    clearTimeout(this.timerId);
    sendFinPacket();
  }

  // The main asynchronous sending loop
  sendLoop() {
    if (this.stats.status !== 'transferring') return;

    this.updateTokens();

    // While we have room in the sliding window and haven't queued all chunks
    while (this.nextSeqNum < this.totalChunks && (this.nextSeqNum - this.windowStart) < this.windowSize) {
      if (this.speedLimit > 0 && this.tokens < this.blockSize) {
        // Out of tokens! Calculate wait time and schedule next loop run
        const tokensNeeded = this.blockSize - this.tokens;
        const waitMs = (tokensNeeded / this.speedLimit) * 1000;
        
        clearTimeout(this.loopTimeoutId);
        this.loopTimeoutId = setTimeout(() => this.sendLoop(), Math.max(1, waitMs));
        return;
      }

      const seq = this.nextSeqNum;
      this.nextSeqNum++;

      // Deduct tokens
      if (this.speedLimit > 0) {
        this.tokens -= this.blockSize;
      }

      this.sendChunk(seq);
    }
  }

  sendChunk(seq) {
    const chunkOffset = seq * this.blockSize;
    const sizeToRead = Math.min(this.blockSize, this.fileSize - chunkOffset);
    const buffer = Buffer.alloc(sizeToRead);

    fs.read(this.fileFd, buffer, 0, sizeToRead, chunkOffset, (err, bytesRead) => {
      if (err || this.stats.status !== 'transferring') return;

      const payload = buffer.slice(0, bytesRead);
      const packet = this.createPacket(PKT_DATA, seq, payload);

      // Record packet info for retransmissions
      this.packets.set(seq, {
        buffer: packet,
        sentTime: Date.now(),
        retransmitCount: 0,
        acked: false
      });

      this.socket.send(packet, 0, packet.length, this.targetPort, this.targetIp);
      
      this.stats.bytesSent += packet.length;
      this.stats.packetsSent++;
    });
  }

  startRetransmitTimer() {
    const checkRetransmissions = () => {
      if (this.stats.status !== 'transferring') return;

      const now = Date.now();
      let needResend = false;

      for (const [seq, info] of this.packets.entries()) {
        // If cumulative ACK already advanced past this sequence, it shouldn't be here,
        // but double check to be safe.
        if (seq < this.windowStart) {
          this.packets.delete(seq);
          continue;
        }

        if (!info.acked && (now - info.sentTime) > this.rto) {
          // Retransmit packet
          info.sentTime = now;
          info.retransmitCount++;
          this.stats.packetsRetransmitted++;
          
          this.socket.send(info.buffer, 0, info.buffer.length, this.targetPort, this.targetIp);
          this.stats.bytesSent += info.buffer.length;
        }
      }

      this.timerId = setTimeout(checkRetransmissions, Math.min(20, this.rto / 2));
    };

    this.timerId = setTimeout(checkRetransmissions, 20);
  }

  setSpeedLimit(bps) {
    this.speedLimit = bps;
    this.emit('log', `Speed limit set to ${bps ? (bps / (1024 * 1024)).toFixed(2) + ' MB/s' : 'Unlimited'}`);
  }

  cleanup(success, message = '') {
    this.isTransferring = false;
    this.stats.status = success ? 'completed' : 'failed';
    this.stats.progress = success ? 100 : this.stats.progress;
    
    clearTimeout(this.timerId);
    clearTimeout(this.loopTimeoutId);
    clearInterval(this.speedIntervalId);

    if (this.fileFd) {
      try {
        fs.closeSync(this.fileFd);
      } catch (e) {}
      this.fileFd = null;
    }

    if (this.socket) {
      try {
        this.socket.close();
      } catch (e) {}
      this.socket = null;
    }

    this.emit('stats', this.stats);
    if (success) {
      this.emit('completed');
    } else {
      this.emit('failed', message);
    }
  }
}

class UdpReceiver extends EventEmitter {
  constructor(options = {}) {
    super();
    this.localPort = options.localPort || 5001;
    
    // Simulated impairments
    this.lossRate = options.lossRate || 0;     // Percentage (0 to 100)
    this.latency = options.latency || 0;       // Delay in ms (one-way)
    
    this.socket = null;
    this.fileFd = null;
    this.outputPath = '';
    this.fileSize = 0;
    this.totalChunks = 0;
    this.blockSize = 0;
    this.sessionId = 0;
    this.senderIp = '';
    this.senderPort = 0;

    // Receive window and buffer
    this.cumulativeSeq = 0;
    this.receivedSeqs = new Set();
    this.pendingWrites = 0;
    this.isFinished = false;

    // Stats
    this.stats = {
      bytesReceived: 0,
      packetsReceived: 0,
      packetsDropped: 0, // Simulated loss
      currentSpeed: 0,
      progress: 0,
      status: 'idle', // idle, waiting, receiving, verifying, completed, failed
      verifiedHash: '',
      expectedHash: '',
      hashMatch: null
    };

    this.speedIntervalId = null;
    this.latencyQueue = [];
  }

  // Parse binary packet
  parsePacket(buffer) {
    if (buffer.length < 11) return null;
    const type = buffer.readUInt8(0);
    const seqNum = buffer.readUInt32BE(1);
    const sessionId = buffer.readUInt32BE(5);
    const length = buffer.readUInt16BE(9);
    const payload = buffer.slice(11, 11 + length);
    return { type, seqNum, sessionId, length, payload };
  }

  createPacket(type, seqNum, payloadBuffer) {
    const payloadLen = payloadBuffer ? payloadBuffer.length : 0;
    const header = Buffer.alloc(11);
    header.writeUInt8(type, 0);
    header.writeUInt32BE(seqNum, 1);
    header.writeUInt32BE(this.sessionId, 5);
    header.writeUInt16BE(payloadLen, 9);
    
    if (payloadBuffer) {
      return Buffer.concat([header, payloadBuffer]);
    }
    return header;
  }

  startListening(outputPath) {
    if (this.socket) return;
    this.outputPath = outputPath;
    this.stats.status = 'waiting';
    this.isFinished = false;
    this.cumulativeSeq = 0;
    this.receivedSeqs.clear();
    
    this.socket = dgram.createSocket('udp4');
    
    this.socket.on('message', (msg, rinfo) => {
      this.senderIp = rinfo.address;
      this.senderPort = rinfo.port;
      
      // Simulate Packet Loss
      if (this.lossRate > 0 && Math.random() * 100 < this.lossRate) {
        this.stats.packetsDropped++;
        // We drop metadata negotiation packets too, except that we might want to log it
        this.stats.bytesReceived += msg.length; // Count it in bandwidth before dropping
        return;
      }

      // Simulate Latency
      if (this.latency > 0) {
        setTimeout(() => {
          this.processIncomingMessage(msg);
        }, this.latency);
      } else {
        this.processIncomingMessage(msg);
      }
    });

    this.socket.on('error', (err) => {
      this.emit('error', err);
      this.cleanup(false, 'Socket error: ' + err.message);
    });

    this.socket.bind(this.localPort, () => {
      this.emit('log', `Receiver UDP socket bound on port ${this.localPort}, waiting for connection...`);
      this.emit('stats', this.stats);
    });

    // Stats speed tracking
    let lastBytes = 0;
    this.speedIntervalId = setInterval(() => {
      const delta = this.stats.bytesReceived - lastBytes;
      this.stats.currentSpeed = delta;
      lastBytes = this.stats.bytesReceived;
      this.stats.progress = this.totalChunks > 0 ? (this.cumulativeSeq / this.totalChunks) * 100 : 0;
      this.emit('stats', this.stats);
    }, 1000);
  }

  processIncomingMessage(msg) {
    if (!this.socket) return;
    this.stats.bytesReceived += msg.length;
    this.stats.packetsReceived++;

    const packet = this.parsePacket(msg);
    if (!packet) return;

    // Handle initial METADATA packet
    if (packet.type === PKT_META) {
      if (this.stats.status === 'waiting' || this.stats.status === 'receiving') {
        try {
          const meta = JSON.parse(packet.payload.toString());
          this.sessionId = packet.sessionId;
          this.fileSize = meta.filesize;
          this.blockSize = meta.blockSize;
          this.totalChunks = meta.totalChunks;
          this.stats.expectedHash = meta.hash;

          this.emit('log', `Negotiating metadata: File Name: ${meta.filename}, Size: ${meta.filesize} bytes, Total Chunks: ${meta.totalChunks}`);
          
          // Open target file for random-access write and truncate to file size
          if (!this.fileFd) {
            this.fileFd = fs.openSync(this.outputPath, 'w+');
            // Pre-allocate disk space for security and performance
            fs.ftruncateSync(this.fileFd, this.fileSize);
            this.emit('log', `Pre-allocated file of size ${this.fileSize} bytes on disk`);
          }

          this.stats.status = 'receiving';
          this.sendAck(PKT_META_ACK, 0);
        } catch (err) {
          this.emit('log', 'Error parsing metadata: ' + err.message);
        }
      }
      return;
    }

    // Verify session
    if (packet.sessionId !== this.sessionId) return;

    if (packet.type === PKT_DATA) {
      const seq = packet.seqNum;
      
      // If already received, just ACK again (sender might have lost previous ACK)
      if (this.receivedSeqs.has(seq)) {
        this.sendAck(PKT_ACK, seq);
        return;
      }

      // Write chunk to correct offset asynchronously
      const writeOffset = seq * this.blockSize;
      this.pendingWrites++;
      
      fs.write(this.fileFd, packet.payload, 0, packet.payload.length, writeOffset, (err) => {
        this.pendingWrites--;
        if (err) {
          this.emit('log', `Disk write error at seq ${seq}: ` + err.message);
          return;
        }

        this.receivedSeqs.add(seq);

        // Update cumulative ACK sequence
        while (this.receivedSeqs.has(this.cumulativeSeq)) {
          this.cumulativeSeq++;
        }

        // Send ACK
        this.sendAck(PKT_ACK, seq);

        // Trigger finish if we got all chunks and disk writes are drained
        if (this.cumulativeSeq >= this.totalChunks && this.pendingWrites === 0 && this.isFinished) {
          this.verifyAndComplete();
        }
      });

    } else if (packet.type === PKT_FIN) {
      this.isFinished = true;
      this.emit('log', 'FIN received. Finalizing disk writes...');
      
      if (this.cumulativeSeq >= this.totalChunks && this.pendingWrites === 0) {
        this.verifyAndComplete();
      } else {
        // Send ACK anyway to let sender know we received FIN, but we're still waiting on some chunks
        this.sendAck(PKT_ACK, this.cumulativeSeq);
      }
    }
  }

  sendAck(type, seqNum) {
    if (!this.socket) return;
    
    // Construct SACK list (Selective ACKs of out-of-order chunks in window)
    const sack = [];
    const maxSackCount = 40; // Limit SACK size to keep ACK packet small
    
    let checked = this.cumulativeSeq + 1;
    while (sack.length < maxSackCount && checked < this.totalChunks) {
      if (this.receivedSeqs.has(checked)) {
        sack.push(checked);
      }
      checked++;
    }

    const ackPayload = Buffer.from(JSON.stringify({
      cumulativeSeq: this.cumulativeSeq,
      sack: sack
    }));

    const response = this.createPacket(type, seqNum, ackPayload);
    
    // Simulate return-path latency if configured
    if (this.latency > 0) {
      setTimeout(() => {
        if (this.socket) {
          this.socket.send(response, 0, response.length, this.senderPort, this.senderIp);
        }
      }, this.latency);
    } else {
      this.socket.send(response, 0, response.length, this.senderPort, this.senderIp);
    }
  }

  async verifyAndComplete() {
    if (this.stats.status === 'completed' || this.stats.status === 'verifying') return;
    
    this.stats.status = 'verifying';
    this.emit('stats', this.stats);
    this.emit('log', 'Verifying checksum hash...');

    // Close FD before reading hash to avoid lock issues
    if (this.fileFd) {
      fs.closeSync(this.fileFd);
      this.fileFd = null;
    }

    try {
      const calculatedHash = await this.calculateHash(this.outputPath);
      this.stats.verifiedHash = calculatedHash;
      this.stats.hashMatch = (calculatedHash === this.stats.expectedHash);
      
      if (this.stats.hashMatch) {
        this.emit('log', `Hash match verified! SHA-256: ${calculatedHash}`);
        this.stats.status = 'completed';
        this.stats.progress = 100;
        
        // Inform sender that we verified successfully
        const response = this.createPacket(PKT_FIN_ACK, this.totalChunks, null);
        if (this.socket) {
          this.socket.send(response, 0, response.length, this.senderPort, this.senderIp, () => {
            setTimeout(() => {
              this.cleanup(true);
            }, 200);
          });
        } else {
          this.cleanup(true);
        }
      } else {
        this.emit('log', `ERROR: Hash mismatch! Expected: ${this.stats.expectedHash}, Got: ${calculatedHash}`);
        this.cleanup(false, 'Checksum mismatch.');
      }
    } catch (err) {
      this.cleanup(false, 'Verification failed: ' + err.message);
    }
  }

  calculateHash(filePath) {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha256');
      const stream = fs.createReadStream(filePath);
      stream.on('data', (data) => hash.update(data));
      stream.on('end', () => resolve(hash.digest('hex')));
      stream.on('error', (err) => reject(err));
    });
  }

  setImpairments(lossRate, latency) {
    this.lossRate = lossRate;
    this.latency = latency;
    this.emit('log', `Network emulation updated: Loss Rate = ${lossRate}%, Latency = ${latency}ms`);
  }

  cleanup(success, message = '') {
    this.stats.status = success ? 'completed' : 'failed';
    this.stats.progress = success ? 100 : this.stats.progress;
    
    clearInterval(this.speedIntervalId);

    if (this.fileFd) {
      try {
        fs.closeSync(this.fileFd);
      } catch (e) {}
      this.fileFd = null;
    }

    if (this.socket) {
      try {
        this.socket.close();
      } catch (e) {}
      this.socket = null;
    }

    this.emit('stats', this.stats);
    if (success) {
      this.emit('completed');
    } else {
      this.emit('failed', message);
    }
  }
}

module.exports = {
  UdpSender,
  UdpReceiver
};
