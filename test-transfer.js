const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { UdpSender, UdpReceiver } = require('./udp-transport');

// Configuration
const TEST_FILE_SIZE = 10 * 1024 * 1024; // 10MB test file
const BLOCK_SIZE = 1400;
const SENDER_PORT = 5020;
const RECEIVER_PORT = 5021;
const LOSS_RATE = 5.0; // 5% packet loss
const LATENCY = 20;    // 20ms one-way latency (40ms RTT)

const srcPath = path.join(__dirname, 'test_source.bin');
const destPath = path.join(__dirname, 'test_dest.bin');

// 1. Generate random test file
console.log(`Generating a random ${TEST_FILE_SIZE / (1024 * 1024)}MB test file at ${srcPath}...`);
const buffer = crypto.randomBytes(TEST_FILE_SIZE);
fs.writeFileSync(srcPath, buffer);

// 2. Initialize Receiver
const receiver = new UdpReceiver({
  localPort: RECEIVER_PORT,
  lossRate: LOSS_RATE,
  latency: LATENCY
});

// 3. Initialize Sender
const sender = new UdpSender({
  targetIp: '127.0.0.1',
  targetPort: RECEIVER_PORT,
  localPort: SENDER_PORT,
  blockSize: BLOCK_SIZE,
  windowSize: 64,
  speedLimit: 5 * 1024 * 1024 // 5 MB/s limit
});

// Statistics
let startTime = 0;

receiver.on('log', (msg) => {
  console.log(`[Receiver] ${msg}`);
});

sender.on('log', (msg) => {
  console.log(`[Sender]   ${msg}`);
});

receiver.on('stats', (stats) => {
  // Silent stats unless needed, or print periodically
});

sender.on('stats', (stats) => {
  if (stats.status === 'transferring') {
    const elapsed = (Date.now() - startTime) / 1000;
    const progress = stats.progress.toFixed(1);
    const speed = (stats.currentSpeed / (1024 * 1024)).toFixed(2);
    process.stdout.write(`\rTransfer progress: ${progress}% | Speed: ${speed} MB/s | RTT: ${stats.rtt}ms | Retransmits: ${stats.packetsRetransmitted}   `);
  }
});

// Run verification when transfer completes
let finished = false;

function verify() {
  if (finished) return;
  finished = true;
  console.log('\n\n--- Transfer Verification ---');

  setTimeout(() => {
    try {
      const srcHash = crypto.createHash('sha256').update(fs.readFileSync(srcPath)).digest('hex');
      const destHash = crypto.createHash('sha256').update(fs.readFileSync(destPath)).digest('hex');

      console.log(`Source File Hash:      ${srcHash}`);
      console.log(`Destination File Hash: ${destHash}`);

      if (srcHash === destHash) {
        console.log('\n✅ SUCCESS: File integrity verified! The hashes match perfectly.');
        console.log(`Stats Summary:`);
        console.log(`- Sent Packets:     ${sender.stats.packetsSent}`);
        console.log(`- Retransmissions:  ${sender.stats.packetsRetransmitted}`);
        console.log(`- Dropped Packets:  ${receiver.stats.packetsDropped}`);
        console.log(`- Loss rate (sim):  ${LOSS_RATE}%`);
        console.log(`- Latency (sim):    ${LATENCY}ms`);
      } else {
        console.log('\n❌ ERROR: Hash mismatch! File corruption detected.');
      }
    } catch (err) {
      console.error('Error verifying files:', err);
    } finally {
      // Clean up files
      try {
        fs.unlinkSync(srcPath);
        fs.unlinkSync(destPath);
        console.log('Temporary test files cleaned up.');
      } catch (e) {}
      process.exit(0);
    }
  }, 1000);
}

receiver.on('completed', () => {
  verify();
});

sender.on('failed', (reason) => {
  console.log(`\nSender failed: ${reason}`);
  verify();
});

receiver.on('failed', (reason) => {
  console.log(`\nReceiver failed: ${reason}`);
  verify();
});

// Start the transfer
startTime = Date.now();
receiver.startListening(destPath);
sender.startTransfer(srcPath);
