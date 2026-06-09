// ============================================================
// WebRTC P2P File Transfer — app.js
// Features: Full-duplex, Pause/Resume, Cancel, Stall detection,
//           Chunk pre-read pipeline, Real-time dual-line chart
// ============================================================

// ── WebSocket / WebRTC globals ─────────────────────────────
const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
const wsUrl = `${protocol}//${window.location.host}`;
let socket;

let peerConnection;
let txChannel; // Channel for sending data (this peer → remote)
let rxChannel; // Channel for receiving data (remote → this peer)
let isInitiator = false;
let roomId = null;

// ── Upload State (Sending) ─────────────────────────────────
let selectedFile = null;
let uploadOffset = 0;
let uploadStartTime = 0;
let uploadLastBytes = 0;
let uploadLastTime = 0;
let isUploading = false;
let isPaused = false;
let lastChunkTime = 0;
let speedLimit = 0; // Bytes per second, 0 = unlimited

// ── Download State (Receiving) ─────────────────────────────
let fileWriter = null;       // FileSystemWritableFileStream or Array (fallback)
let downloadedBlob = null;
let downloadReceivedSize = 0;
let downloadFileSize = 0;
let downloadFileName = '';
let downloadStartTime = 0;
let downloadLastBytes = 0;
let downloadLastTime = 0;
let isDownloading = false;

// ── Write queue (prevent concurrent writes to FileSystemWritableFileStream) ──
let writeQueue = [];
let isWriting = false;

// ── Chunk pipeline ─────────────────────────────────────────
let nextChunkBuffer = null;  // Pre-read next chunk while sending current
let isReadingNext = false;

// ── Protocol settings ─────────────────────────────────────
let chunkSize = 16384;
let lastRtt = 0;
const MAX_BUFFERED_AMOUNT = 1048576; // 1 MB back-pressure threshold

// ── Stall detection ───────────────────────────────────────
let txStallTimer = null;
let rxStallTimer = null;
let txLastProgressBytes = 0;
let rxLastProgressBytes = 0;
const STALL_TIMEOUT_MS = 8000;

// ── Chart state ────────────────────────────────────────────
let chart = null;
let chartTimeCounter = 0;
const maxChartPoints = 30;
const chartTimeLabels = [];
const chartTxSpeedData = [];
const chartRxSpeedData = [];
const chartRttData = [];

// ── Stats interval ─────────────────────────────────────────
let statsInterval = null;

// ============================================================
// DOM Elements
// ============================================================
const elRoomSetup = document.getElementById('room-setup-section');
const elTransferSection = document.getElementById('transfer-section');
const elCurrentRoomId = document.getElementById('current-room-id');
const elPeerRoleText = document.getElementById('peer-role-text');
const elFileInput = document.getElementById('file-input');
const elBtnStartSend = document.getElementById('btn-start-send');
const elBtnDisconnect = document.getElementById('btn-disconnect');

// Upload UI
const elUploadStatsPanel = document.getElementById('upload-stats-panel');
const elTxSpeed = document.getElementById('tx-speed');
const elTxRtt = document.getElementById('tx-rtt');
const elTxSent = document.getElementById('tx-sent');
const elTxTime = document.getElementById('tx-time');
const elTxProgressBar = document.getElementById('tx-progress-bar');
const elTxProgressPercent = document.getElementById('tx-progress-percent');
const elTxStatusText = document.getElementById('tx-status-text');
const elTxStallWarning = document.getElementById('tx-stall-warning');
const elBtnPauseSend = document.getElementById('btn-pause-send');
const elBtnResumeSend = document.getElementById('btn-resume-send');
const elBtnCancelSend = document.getElementById('btn-cancel-send');

// Download UI
const elDownloadStatsPanel = document.getElementById('download-stats-panel');
const elRxSpeed = document.getElementById('rx-speed');
const elRxReceived = document.getElementById('rx-received');
const elRxTime = document.getElementById('rx-time');
const elRxProgressBar = document.getElementById('rx-progress-bar');
const elRxProgressPercent = document.getElementById('rx-progress-percent');
const elRxStatusText = document.getElementById('rx-status-text');
const elRxIntegrityStatus = document.getElementById('rx-integrity-status');
const elRxStallWarning = document.getElementById('rx-stall-warning');
const elIncomingFileAlert = document.getElementById('incoming-file-alert');
const elIncomingFilename = document.getElementById('incoming-filename');
const elIncomingFilesize = document.getElementById('incoming-filesize');
const elBtnAcceptFile = document.getElementById('btn-accept-file');
const elNoIncomingPrompt = document.getElementById('no-incoming-prompt');
const elBtnCancelRecv = document.getElementById('btn-cancel-recv');
const elFallbackDownloadContainer = document.getElementById('fallback-download-container');
const elBtnFallbackDownload = document.getElementById('btn-fallback-download');

// Settings UI
const elChunkSizeSelect = document.getElementById('chunk-size-select');
const elTxChunkSize = document.getElementById('tx-chunk-size');
const elProtocolAnalysis = document.getElementById('protocol-analysis');
const elConsoleLogs = document.getElementById('console-logs');
const speedLimitInput = document.getElementById('speedLimit');
const speedLimitVal = document.getElementById('speed-limit-val');

// ============================================================
// STUN Configuration
// ============================================================
const rtcConfig = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' }
  ]
};

// ============================================================
// WebSocket Signaling
// ============================================================
function connectWebSocket() {
  socket = new WebSocket(wsUrl);

  socket.onopen = () => {
    log('Connected to signaling server.');
    checkURLParams();
  };

  socket.onclose = () => {
    log('Disconnected from signaling server. Retrying in 2s...');
    setTimeout(connectWebSocket, 2000);
  };

  socket.onmessage = async (event) => {
    const message = JSON.parse(event.data);

    switch (message.type) {
      case 'joined':
        roomId = message.room;
        isInitiator = message.isInitiator;
        elCurrentRoomId.textContent = roomId;
        elRoomSetup.classList.add('hidden');
        elTransferSection.classList.remove('hidden');
        log(`Joined room ${roomId} as ${isInitiator ? 'initiator' : 'receiver'}.`);
        
        // Setup QR code & copy link button
        const shareUrl = `${window.location.origin}${window.location.pathname}?room=${roomId}`;
        const btnCopy = document.getElementById('btn-copy-link');
        const shareControls = document.getElementById('share-controls');
        
        if (shareControls) {
          shareControls.style.display = isInitiator ? 'flex' : 'none';
        }
        
        if (isInitiator && window.QRCode) {
          const qrcodeContainer = document.getElementById('qrcode');
          if (qrcodeContainer) {
            qrcodeContainer.innerHTML = '';
            new QRCode(qrcodeContainer, {
              text: shareUrl,
              width: 128,
              height: 128,
              colorDark : "#000000",
              colorLight : "#ffffff",
              correctLevel : QRCode.CorrectLevel.H
            });
          }
        }
        
        if (btnCopy) {
          btnCopy.onclick = () => {
            navigator.clipboard.writeText(shareUrl).then(() => {
              btnCopy.textContent = '✓ Copied!';
              setTimeout(() => { btnCopy.textContent = '📋 Copy Share Link'; }, 2000);
            }).catch(() => {
              alert('Could not copy automatically. URL is: ' + shareUrl);
            });
          };
        }

        if (isInitiator) {
          elPeerRoleText.innerHTML = `Waiting for peer to join...<br><small>Share link: ${shareUrl}</small>`;
        } else {
          elPeerRoleText.textContent = 'Connecting to peer...';
        }
        break;

      case 'ready':
        log('Peer connected. Establishing WebRTC connection...');
        elPeerRoleText.textContent = 'Establishing secure connection...';
        initiateWebRTC();
        break;

      case 'signal':
        if (message.data.ecdhPubkey) {
          log('Received remote ECDH public key. Deriving shared AES-256 key...');
          try {
            await E2E.deriveSharedKey(message.data.ecdhPubkey);
            log('🔒 End-to-End Encrypted Channel established successfully!');
            updateE2EUI('active', 'E2E Active');
          } catch (e) {
            log(`❌ Failed to derive shared secret: ${e.message}`);
            updateE2EUI('error', 'E2E Error');
          }
          break;
        }

        if (peerConnection) {
          try {
            if (message.data.sdp) {
              await peerConnection.setRemoteDescription(new RTCSessionDescription(message.data.sdp));
              if (peerConnection.remoteDescription.type === 'offer') {
                const answer = await peerConnection.createAnswer();
                await peerConnection.setLocalDescription(answer);
                sendSignal({ sdp: peerConnection.localDescription });
              }
            } else if (message.data.candidate) {
              await peerConnection.addIceCandidate(new RTCIceCandidate(message.data.candidate));
            }
          } catch (e) {
            log(`Error handling signal: ${e.message}`);
          }
        }
        break;

      case 'peer-left':
        log('Peer disconnected.');
        // Tear down WebRTC so a fresh session can be created when peer rejoins
        cleanupWebRTC();
        updateConnectionUI('idle', 'Disconnected');
        elPeerRoleText.innerHTML = `Peer left. Waiting for new peer...<br><small>Share link: ${window.location.origin}?room=${roomId}</small>`;
        break;

      case 'full':
        alert('Room is full. Please try another room.');
        break;

      case 'error':
        log(`Server error: ${message.message}`);
        break;
    }
  };
}

function checkURLParams() {
  const urlParams = new URLSearchParams(window.location.search);
  const roomParam = urlParams.get('room');
  if (roomParam) {
    document.getElementById('room-id-input').value = roomParam;
    joinRoom(roomParam);
  }
}

function joinRoom(room) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: 'join', room }));
  }
}

function sendSignal(data) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({ type: 'signal', room: roomId, data }));
  }
}

// ============================================================
// WebRTC Setup
// ============================================================
async function initiateWebRTC() {
  // Initialize E2E key exchange
  if (window.E2E) {
    try {
      E2E.reset();
      updateE2EUI('pending', 'E2E Pending');
      log('Generating ECDH key pair for E2E encryption...');
      const myPub = await E2E.init();
      log('ECDH key pair generated. Sharing public key...');
      sendSignal({ ecdhPubkey: myPub });
    } catch (err) {
      log(`❌ E2E Encryption init failed: ${err.message}`);
      updateE2EUI('error', 'E2E Error');
    }
  }

  peerConnection = new RTCPeerConnection(rtcConfig);

  peerConnection.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignal({ candidate: event.candidate });
    }
  };

  peerConnection.onconnectionstatechange = () => {
    const state = peerConnection.connectionState;
    log(`Connection state: ${state}`);

    if (state === 'connected') {
      updateConnectionUI('running', 'Connected P2P');
      elPeerRoleText.textContent = 'Peer Connected. Ready to transfer.';
      elFileInput.disabled = false;
      speedLimitInput.disabled = false;
      if (elChunkSizeSelect) elChunkSizeSelect.disabled = false;
    } else if (state === 'disconnected' || state === 'failed') {
      updateConnectionUI('error', 'Disconnected');
      cleanupWebRTC();
    }
  };

  if (isInitiator) {
    // Initiator creates BOTH channels (full duplex)
    txChannel = peerConnection.createDataChannel('send-channel', { ordered: true });
    rxChannel = peerConnection.createDataChannel('recv-channel', { ordered: true });
    setupTxChannel(txChannel);
    setupRxChannel(rxChannel);

    try {
      const offer = await peerConnection.createOffer();
      await peerConnection.setLocalDescription(offer);
      sendSignal({ sdp: peerConnection.localDescription });
    } catch (e) {
      log(`Failed to create offer: ${e.message}`);
    }
  } else {
    // Receiver waits for channels
    peerConnection.ondatachannel = (event) => {
      const ch = event.channel;
      if (ch.label === 'send-channel') {
        rxChannel = ch;
        setupRxChannel(rxChannel);
      } else if (ch.label === 'recv-channel') {
        txChannel = ch;
        setupTxChannel(txChannel);
      }
    };
  }
}

function cleanupWebRTC() {
  stopStallTimers();
  if (txChannel) { try { txChannel.close(); } catch (e) {} txChannel = null; }
  if (rxChannel) { try { rxChannel.close(); } catch (e) {} rxChannel = null; }
  if (peerConnection) { try { peerConnection.close(); } catch (e) {} peerConnection = null; }
  elFileInput.disabled = true;
  speedLimitInput.disabled = true;
  if (elChunkSizeSelect) elChunkSizeSelect.disabled = true;

  if (window.E2E) {
    E2E.reset();
    updateE2EUI('pending', 'E2E Pending');
  }
}

function updateConnectionUI(state, text) {
  const dot = document.getElementById('connection-status-dot');
  const txt = document.getElementById('connection-status-text');
  dot.className = `pulse-dot ${state}`;
  txt.textContent = text;
}

function updateE2EUI(status, text) {
  const badge = document.getElementById('e2e-badge');
  const icon = document.getElementById('e2e-icon');
  const label = document.getElementById('e2e-label');
  if (!badge) return;
  badge.className = `e2e-badge e2e-${status}`;
  if (status === 'active') {
    icon.textContent = '🔒';
    label.textContent = text || 'E2E Active';
  } else if (status === 'error') {
    icon.textContent = '⚠️';
    label.textContent = text || 'E2E Error';
  } else {
    icon.textContent = '🔓';
    label.textContent = text || 'E2E Pending';
  }
}

// ============================================================
// Transmit Channel Setup
// ============================================================
function setupTxChannel(channel) {
  channel.binaryType = 'arraybuffer';

  channel.onopen = () => {
    log('Transmit data channel opened.');
    elFileInput.disabled = false;
    speedLimitInput.disabled = false;
  };

  channel.onclose = () => log('Transmit data channel closed.');

  // Control messages FROM receiver arrive on txChannel
  channel.onmessage = (event) => {
    if (typeof event.data !== 'string') return;
    const msg = JSON.parse(event.data);

    if (msg.type === 'ready') {
      log('Receiver is ready. Starting file transfer...');
      startStreaming();
    } else if (msg.type === 'cancel') {
      log('Receiver cancelled the transfer.');
      stopTxStallTimer();
      resetUploadUI();
      elTxStatusText.textContent = 'Cancelled by receiver';
    }
  };
}

// ============================================================
// Receive Channel Setup
// ============================================================
function setupRxChannel(channel) {
  channel.binaryType = 'arraybuffer';

  channel.onopen = () => log('Receive data channel opened.');
  channel.onclose = () => log('Receive data channel closed.');

  channel.onmessage = async (event) => {
    if (typeof event.data === 'string') {
      const msg = JSON.parse(event.data);

      if (msg.type === 'meta') {
        // Incoming file offer
        log(`Incoming file offer: ${msg.name} (${formatBytes(msg.size)})`);
        downloadFileName = msg.name;
        downloadFileSize = msg.size;

        elIncomingFilename.textContent = msg.name;
        elIncomingFilesize.textContent = formatBytes(msg.size);

        elDownloadStatsPanel.classList.add('hidden');
        if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.add('hidden');
        downloadedBlob = null;

        elNoIncomingPrompt.classList.add('hidden');
        elIncomingFileAlert.classList.remove('hidden');

        // Accept button — prompts save location then signals sender to start
        elBtnAcceptFile.onclick = async () => {
          elIncomingFileAlert.classList.add('hidden');
          elDownloadStatsPanel.classList.remove('hidden');
          if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.add('hidden');
          elRxStatusText.textContent = 'Saving...';
          elRxIntegrityStatus.textContent = 'Downloading...';

          try {
            if ('showSaveFilePicker' in window) {
              try {
                const handle = await window.showSaveFilePicker({ suggestedName: downloadFileName });
                fileWriter = await handle.createWritable();
                log('File stream opened via File System Access API (direct-to-disk).');
              } catch (e) {
                log(`Save picker cancelled/failed: ${e.message}. Using in-memory buffer.`);
                fileWriter = [];
              }
            } else {
              log('File System Access API not supported. Using in-memory buffer.');
              fileWriter = [];
            }

            downloadReceivedSize = 0;
            isDownloading = true;
            downloadStartTime = performance.now();
            downloadLastTime = downloadStartTime;
            downloadLastBytes = 0;
            rxLastProgressBytes = 0;

            // Signal sender we are ready
            if (rxChannel && rxChannel.readyState === 'open') {
              rxChannel.send(JSON.stringify({ type: 'ready' }));
            }

            startStatsInterval();
            startRxStallTimer();
          } catch (e) {
            log(`Save setup failed: ${e.message}`);
            if (rxChannel && rxChannel.readyState === 'open') {
              rxChannel.send(JSON.stringify({ type: 'cancel' }));
            }
            resetDownloadUI();
          }
        };

      } else if (msg.type === 'done') {
        log('All chunks received. Finalizing file...');
        isDownloading = false;
        stopRxStallTimer();

        if (writeQueue.length === 0 && !isWriting) {
          await closeFileWriter();
        }
        // If writes are still in progress, processWriteQueue will call closeFileWriter when done.
      } else if (msg.type === 'cancel') {
        log('Sender cancelled the transfer.');
        stopRxStallTimer();
        resetDownloadUI();
        elRxStatusText.textContent = 'Cancelled by sender';
      }

    } else {
      // Binary chunk received
      const encChunk = event.data;
      let decChunk;

      if (window.E2E && E2E.ready()) {
        try {
          if (downloadReceivedSize === 0) {
            log('🔒 E2E: Decrypting incoming chunks using AES-256-GCM...');
          }
          decChunk = await E2E.decryptChunk(encChunk);
        } catch (err) {
          log(`❌ Decryption error: ${err.message}`);
          cancelRecv();
          return;
        }
      } else {
        log('⚠️ E2E decryption is not ready. Aborting transfer.');
        cancelRecv();
        return;
      }

      downloadReceivedSize += decChunk.byteLength;
      writeChunk(decChunk);

      // Reset stall timer — we got data
      resetRxStallTimer();

      // Fast progress bar update
      const pct = ((downloadReceivedSize / downloadFileSize) * 100).toFixed(1);
      elRxProgressBar.style.width = `${pct}%`;
      elRxProgressPercent.textContent = `${pct}%`;
    }
  };
}

// ============================================================
// Write Queue — Serializes all disk writes
// ============================================================
async function writeChunk(chunk) {
  if (!fileWriter) return;
  if (typeof fileWriter.write === 'function') {
    // FileSystemWritableFileStream: queue for serialized writes
    writeQueue.push(chunk);
    processWriteQueue();
  } else if (Array.isArray(fileWriter)) {
    // In-memory fallback: just push
    fileWriter.push(chunk);
  }
}

async function processWriteQueue() {
  if (isWriting || writeQueue.length === 0) return;
  isWriting = true;

  while (writeQueue.length > 0) {
    const chunk = writeQueue.shift();
    try {
      if (fileWriter && typeof fileWriter.write === 'function') {
        await fileWriter.write(chunk);
      }
    } catch (e) {
      log(`Disk write error: ${e.message}`);
      isWriting = false;
      // Notify sender and abort
      if (rxChannel && rxChannel.readyState === 'open') {
        rxChannel.send(JSON.stringify({ type: 'cancel' }));
      }
      await resetDownloadUI();
      return;
    }
  }

  isWriting = false;

  // If download signalled 'done' and queue is now drained, close the file
  if (!isDownloading && writeQueue.length === 0 && fileWriter) {
    await closeFileWriter();
  }
}

async function closeFileWriter() {
  const writer = fileWriter;
  fileWriter = null; // Clear reference before async work to prevent re-entry

  if (writer && typeof writer.close === 'function') {
    log('Closing file stream and saving to disk...');
    try {
      await writer.close();
      log('✅ File saved successfully to disk.');
      elRxStatusText.textContent = 'Saved ✓';
      elRxIntegrityStatus.textContent = 'File saved successfully.';
    } catch (e) {
      log(`Error closing file: ${e.message}`);
      elRxStatusText.textContent = 'Error';
      elRxIntegrityStatus.textContent = `Error saving file: ${e.message}`;
    }
  } else if (Array.isArray(writer)) {
    log('Assembling in-memory download...');
    try {
      downloadedBlob = new Blob(writer);
      elRxStatusText.textContent = 'Ready to Save';
      elRxIntegrityStatus.textContent = 'Transfer complete. Click below to save.';
      if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.remove('hidden');
      if (elBtnFallbackDownload) {
        elBtnFallbackDownload.onclick = () => {
          try {
            const url = URL.createObjectURL(downloadedBlob);
            const a = document.createElement('a');
            a.href = url;
            a.download = downloadFileName;
            a.click();
            URL.revokeObjectURL(url);
            log('File downloaded via user gesture.');
          } catch (err) {
            log(`Error downloading file: ${err.message}`);
          }
          if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.add('hidden');
          downloadedBlob = null;
        };
      }
    } catch (e) {
      log(`Error assembling in-memory file: ${e.message}`);
      elRxStatusText.textContent = 'Error';
      elRxIntegrityStatus.textContent = 'Failed to assemble file.';
    }
  }
}

// ============================================================
// Sending: Offer, Stream, Pause/Resume, Cancel
// ============================================================
function startSendingFile() {
  if (window.E2E && !E2E.ready()) {
    alert('End-to-end secure channel is still establishing. Please wait a moment.');
    return;
  }
  log(`Offering to send: ${selectedFile.name} (${formatBytes(selectedFile.size)})`);
  txChannel.send(JSON.stringify({
    type: 'meta',
    name: selectedFile.name,
    size: selectedFile.size
  }));
  elUploadStatsPanel.classList.remove('hidden');
  elTxStatusText.textContent = 'Waiting for peer approval...';
  // Show pause/cancel controls, hide resume
  elBtnPauseSend.style.display = '';
  elBtnResumeSend.style.display = 'none';
}

async function startStreaming() {
  uploadOffset = 0;
  isUploading = true;
  isPaused = false;
  uploadStartTime = performance.now();
  uploadLastTime = uploadStartTime;
  uploadLastBytes = 0;
  txLastProgressBytes = 0;
  nextChunkBuffer = null;
  isReadingNext = false;

  if (elTxChunkSize) elTxChunkSize.textContent = `${(chunkSize / 1024).toFixed(0)} KB`;

  elTxStatusText.textContent = 'Transmitting...';
  elBtnPauseSend.style.display = '';
  elBtnResumeSend.style.display = 'none';

  startStatsInterval();
  startTxStallTimer();

  // Backpressure: resume sending when buffer drains
  txChannel.bufferedAmountLowThreshold = 65536;
  txChannel.onbufferedamountlow = () => {
    if (!isPaused) sendNextChunks();
  };

  // Pre-read the first chunk immediately
  await preReadNextChunk();
  sendNextChunks();
}

/**
 * Pre-reads the next chunk from disk into nextChunkBuffer so it's
 * ready to send as soon as the current chunk has been transmitted.
 */
async function preReadNextChunk() {
  const offset = uploadOffset + (nextChunkBuffer ? nextChunkBuffer.byteLength : 0);
  if (offset >= selectedFile.size || isReadingNext) return;
  isReadingNext = true;
  const slice = selectedFile.slice(offset, offset + chunkSize);
  nextChunkBuffer = await slice.arrayBuffer();
  isReadingNext = false;
}

async function sendNextChunks() {
  if (!isUploading || isPaused) return;

  while (uploadOffset < selectedFile.size) {
    if (txChannel.bufferedAmount > MAX_BUFFERED_AMOUNT) {
      // Back-pressure: wait for onbufferedamountlow
      return;
    }

    // Speed throttling
    if (speedLimit > 0) {
      const now = performance.now();
      const elapsed = now - lastChunkTime;
      const minTime = (chunkSize / speedLimit) * 1000;
      if (elapsed < minTime) {
        setTimeout(() => { if (!isPaused) sendNextChunks(); }, minTime - elapsed);
        return;
      }
    }

    // Use pre-read buffer if available, otherwise read now
    let buffer;
    if (nextChunkBuffer) {
      buffer = nextChunkBuffer;
      nextChunkBuffer = null;
    } else {
      const chunk = selectedFile.slice(uploadOffset, uploadOffset + chunkSize);
      buffer = await chunk.arrayBuffer();
    }

    let encryptedBuffer = buffer;
    if (window.E2E && E2E.ready()) {
      try {
        if (uploadOffset === 0) {
          log('🔒 E2E: Encrypting outgoing chunks using AES-256-GCM...');
        }
        encryptedBuffer = await E2E.encryptChunk(buffer);
      } catch (err) {
        log(`❌ Encryption error: ${err.message}`);
        cancelSend();
        return;
      }
    } else {
      log('⚠️ E2E encryption is not ready. Aborting transfer.');
      cancelSend();
      return;
    }

    txChannel.send(encryptedBuffer);
    uploadOffset += buffer.byteLength;
    lastChunkTime = performance.now();

    // Update progress UI
    const pct = ((uploadOffset / selectedFile.size) * 100).toFixed(1);
    elTxProgressBar.style.width = `${pct}%`;
    elTxProgressPercent.textContent = `${pct}%`;
    if (elTxChunkSize) elTxChunkSize.textContent = `${(chunkSize / 1024).toFixed(0)} KB`;

    // Reset stall timer — we just sent data
    resetTxStallTimer();

    // Pre-read the next chunk in background (pipeline)
    preReadNextChunk();
  }

  if (uploadOffset >= selectedFile.size) {
    txChannel.send(JSON.stringify({ type: 'done' }));
    log('Upload complete. Sent "done" signal to receiver.');
    elTxStatusText.textContent = 'Done ✓';
    isUploading = false;
    isPaused = false;
    stopTxStallTimer();
    elBtnPauseSend.style.display = 'none';
    elBtnResumeSend.style.display = 'none';
  }
}

function pauseTransfer() {
  if (!isUploading || isPaused) return;
  isPaused = true;
  elTxStatusText.textContent = 'Paused';
  elBtnPauseSend.style.display = 'none';
  elBtnResumeSend.style.display = '';
  stopTxStallTimer(); // Don't false-alarm during intentional pause
  log('Transfer paused.');
}

function resumeTransfer() {
  if (!isUploading || !isPaused) return;
  isPaused = false;
  elTxStatusText.textContent = 'Transmitting...';
  elBtnPauseSend.style.display = '';
  elBtnResumeSend.style.display = 'none';
  txLastProgressBytes = uploadOffset; // Reset stall baseline
  startTxStallTimer();
  log('Transfer resumed.');
  sendNextChunks();
}

function cancelSend() {
  if (!isUploading && !isPaused) return;
  log('Transfer cancelled by sender.');
  isUploading = false;
  isPaused = false;
  stopTxStallTimer();
  // Tell receiver we cancelled
  if (txChannel && txChannel.readyState === 'open') {
    txChannel.send(JSON.stringify({ type: 'cancel' }));
  }
  resetUploadUI();
}

function cancelRecv() {
  if (!isDownloading) return;
  log('Transfer cancelled by receiver.');
  isDownloading = false;
  stopRxStallTimer();
  // Tell sender we cancelled
  if (rxChannel && rxChannel.readyState === 'open') {
    rxChannel.send(JSON.stringify({ type: 'cancel' }));
  }
  resetDownloadUI();
}

// ============================================================
// Stall Detection Watchdog
// ============================================================
function startTxStallTimer() {
  stopTxStallTimer();
  txLastProgressBytes = uploadOffset;
  txStallTimer = setInterval(() => {
    if (!isUploading || isPaused) { stopTxStallTimer(); return; }
    if (uploadOffset === txLastProgressBytes) {
      // No progress in the last STALL_TIMEOUT_MS
      if (elTxStallWarning) elTxStallWarning.classList.remove('hidden');
      log(`⚠ TX stall detected — no data sent in ${STALL_TIMEOUT_MS / 1000}s`);
    } else {
      if (elTxStallWarning) elTxStallWarning.classList.add('hidden');
    }
    txLastProgressBytes = uploadOffset;
  }, STALL_TIMEOUT_MS);
}

function resetTxStallTimer() {
  txLastProgressBytes = uploadOffset;
}

function stopTxStallTimer() {
  if (txStallTimer) { clearInterval(txStallTimer); txStallTimer = null; }
  if (elTxStallWarning) elTxStallWarning.classList.add('hidden');
}

function startRxStallTimer() {
  stopRxStallTimer();
  rxLastProgressBytes = downloadReceivedSize;
  rxStallTimer = setInterval(() => {
    if (!isDownloading) { stopRxStallTimer(); return; }
    if (downloadReceivedSize === rxLastProgressBytes) {
      if (elRxStallWarning) elRxStallWarning.classList.remove('hidden');
      log(`⚠ RX stall detected — no data received in ${STALL_TIMEOUT_MS / 1000}s`);
    } else {
      if (elRxStallWarning) elRxStallWarning.classList.add('hidden');
    }
    rxLastProgressBytes = downloadReceivedSize;
  }, STALL_TIMEOUT_MS);
}

function resetRxStallTimer() {
  rxLastProgressBytes = downloadReceivedSize;
}

function stopRxStallTimer() {
  if (rxStallTimer) { clearInterval(rxStallTimer); rxStallTimer = null; }
  if (elRxStallWarning) elRxStallWarning.classList.add('hidden');
}

function stopStallTimers() {
  stopTxStallTimer();
  stopRxStallTimer();
}

// ============================================================
// Stats Interval — Real-time metrics, 1s tick
// ============================================================
function startStatsInterval() {
  if (statsInterval) return;

  statsInterval = setInterval(async () => {
    if (!isUploading && !isDownloading) {
      clearInterval(statsInterval);
      statsInterval = null;
      return;
    }

    const now = performance.now();
    let txSpeedMbps = 0;
    let rxSpeedMbps = 0;

    // ── TX Stats ────────────────────────────────────────────
    if (isUploading) {
      const timeDelta = (now - uploadLastTime) / 1000;
      const byteDelta = uploadOffset - uploadLastBytes;
      const speedBytesSec = timeDelta > 0 ? byteDelta / timeDelta : 0;
      txSpeedMbps = (speedBytesSec * 8) / (1024 * 1024);

      elTxSpeed.textContent = `${(speedBytesSec / (1024 * 1024)).toFixed(2)} MB/s (${txSpeedMbps.toFixed(1)} Mbps)`;
      elTxSent.textContent = `${(uploadOffset / (1024 * 1024)).toFixed(1)} MB / ${(selectedFile.size / (1024 * 1024)).toFixed(1)} MB`;

      if (speedBytesSec > 0) {
        const remaining = Math.round((selectedFile.size - uploadOffset) / speedBytesSec);
        elTxTime.textContent = formatTime(remaining);
      } else {
        elTxTime.textContent = isPaused ? 'Paused' : '--:--';
      }

      uploadLastBytes = uploadOffset;
      uploadLastTime = now;
    }

    // ── RX Stats ────────────────────────────────────────────
    if (isDownloading) {
      const timeDelta = (now - downloadLastTime) / 1000;
      const byteDelta = downloadReceivedSize - downloadLastBytes;
      const speedBytesSec = timeDelta > 0 ? byteDelta / timeDelta : 0;
      rxSpeedMbps = (speedBytesSec * 8) / (1024 * 1024);

      elRxSpeed.textContent = `${(speedBytesSec / (1024 * 1024)).toFixed(2)} MB/s (${rxSpeedMbps.toFixed(1)} Mbps)`;
      elRxReceived.textContent = `${(downloadReceivedSize / (1024 * 1024)).toFixed(1)} MB / ${(downloadFileSize / (1024 * 1024)).toFixed(1)} MB`;

      if (speedBytesSec > 0) {
        const remaining = Math.round((downloadFileSize - downloadReceivedSize) / speedBytesSec);
        elRxTime.textContent = formatTime(remaining);
      } else {
        elRxTime.textContent = '--:--';
      }

      downloadLastBytes = downloadReceivedSize;
      downloadLastTime = now;
    }

    // ── RTT (real, from WebRTC stats) ───────────────────────
    let rtt = 0;
    if (peerConnection) {
      try {
        const statsReport = await peerConnection.getStats();
        statsReport.forEach(report => {
          if (report.type === 'candidate-pair' && report.state === 'succeeded') {
            rtt = (report.currentRoundTripTime || 0) * 1000;
          }
        });
      } catch (e) {}
    }
    elTxRtt.textContent = rtt > 0 ? `${rtt.toFixed(0)} ms` : 'N/A';
    lastRtt = rtt;
    updateProtocolAnalysis();

    // ── Chart update (dual-line: TX + RX) ───────────────────
    pushChartData(txSpeedMbps, rxSpeedMbps, rtt > 0 ? rtt : null);

  }, 1000);
}

// ============================================================
// UI Reset helpers
// ============================================================
function resetUploadUI() {
  isUploading = false;
  isPaused = false;
  stopTxStallTimer();
  elUploadStatsPanel.classList.add('hidden');
  elTxProgressBar.style.width = '0%';
  elTxProgressPercent.textContent = '0%';
  elTxStatusText.textContent = 'Ready';
  if (elBtnPauseSend) { elBtnPauseSend.style.display = ''; }
  if (elBtnResumeSend) { elBtnResumeSend.style.display = 'none'; }
  nextChunkBuffer = null;
}

async function resetDownloadUI() {
  isDownloading = false;
  stopRxStallTimer();
  elDownloadStatsPanel.classList.add('hidden');
  elIncomingFileAlert.classList.add('hidden');
  elNoIncomingPrompt.classList.remove('hidden');
  elRxProgressBar.style.width = '0%';
  elRxProgressPercent.textContent = '0%';
  if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.add('hidden');
  if (elBtnFallbackDownload) elBtnFallbackDownload.onclick = null;
  downloadedBlob = null;

  const writer = fileWriter;
  fileWriter = null;
  writeQueue = [];
  isWriting = false;

  // Abort the FileSystemWritableFileStream to clean up the .crswap temp file
  if (writer && typeof writer.abort === 'function') {
    log('Aborting file stream (cleaning up .crswap temp file)...');
    try {
      await writer.abort();
      log('Temp file cleanup complete.');
    } catch (e) {
      log(`Error aborting file stream: ${e.message}`);
    }
  }
}

function resetUI() {
  resetUploadUI();
  resetDownloadUI();
  cleanupWebRTC();
  elRoomSetup.classList.remove('hidden');
  elTransferSection.classList.add('hidden');
  updateConnectionUI('idle', 'Disconnected');
  const shareControls = document.getElementById('share-controls');
  if (shareControls) shareControls.style.display = 'none';
}

// ============================================================
// Protocol Analysis Tip
// ============================================================
function updateProtocolAnalysis() {
  if (!elProtocolAnalysis) return;
  let tip = '<strong>Optimal Config Tip:</strong> ';
  if (lastRtt > 150) {
    tip += `High latency (${lastRtt.toFixed(0)} ms). Use 64–256 KB chunks for better long-distance throughput.`;
  } else if (lastRtt > 0) {
    tip += `Stable link (RTT: ${lastRtt.toFixed(0)} ms). 16–32 KB chunks are optimal.`;
  } else {
    tip += `16 KB chunk size recommended for most conditions.`;
  }
  elProtocolAnalysis.innerHTML = tip;
}

// ============================================================
// Utility
// ============================================================
function log(msg) {
  const time = new Date().toLocaleTimeString();
  elConsoleLogs.textContent += `\n[${time}] ${msg}`;
  elConsoleLogs.scrollTop = elConsoleLogs.scrollHeight;
}

function clearLogs() {
  elConsoleLogs.textContent = 'Logs cleared.';
}

function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals < 0 ? 0 : decimals)) + ' ' + sizes[i];
}

function formatTime(seconds) {
  if (seconds === Infinity || isNaN(seconds) || seconds < 0) return '--:--';
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}:${secs < 10 ? '0' : ''}${secs}`;
}

// ============================================================
// Chart.js — Dual-line (TX + RX speed) + RTT
// ============================================================
function initChart() {
  const ctx = document.getElementById('performance-chart').getContext('2d');
  chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: chartTimeLabels,
      datasets: [
        {
          label: 'TX Speed (Mbps)',
          borderColor: 'rgba(99, 102, 241, 1)',
          backgroundColor: 'rgba(99, 102, 241, 0.06)',
          borderWidth: 2,
          data: chartTxSpeedData,
          yAxisID: 'y',
          tension: 0.3,
          fill: true
        },
        {
          label: 'RX Speed (Mbps)',
          borderColor: 'rgba(16, 185, 129, 1)',
          backgroundColor: 'rgba(16, 185, 129, 0.06)',
          borderWidth: 2,
          data: chartRxSpeedData,
          yAxisID: 'y',
          tension: 0.3,
          fill: true
        },
        {
          label: 'RTT (ms)',
          borderColor: 'rgba(168, 85, 247, 1)',
          backgroundColor: 'transparent',
          borderWidth: 1.5,
          borderDash: [5, 5],
          data: chartRttData,
          yAxisID: 'y-rtt',
          tension: 0.1
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false, // Disable animation for real-time performance
      scales: {
        x: {
          grid: { color: 'rgba(255,255,255,0.03)' },
          ticks: { color: '#64748b' }
        },
        y: {
          type: 'linear',
          display: true,
          position: 'left',
          title: { display: true, text: 'Speed (Mbps)', color: 'rgba(99, 102, 241, 0.8)' },
          grid: { color: 'rgba(255,255,255,0.05)' },
          ticks: { color: '#64748b' },
          min: 0
        },
        'y-rtt': {
          type: 'linear',
          display: true,
          position: 'right',
          title: { display: true, text: 'RTT (ms)', color: 'rgba(168, 85, 247, 0.8)' },
          grid: { drawOnChartArea: false },
          ticks: { color: '#64748b' },
          min: 0
        }
      },
      plugins: {
        legend: {
          display: true,
          labels: { color: '#94a3b8', boxWidth: 12, font: { size: 11 } }
        }
      }
    }
  });
}

function pushChartData(txMbps, rxMbps, rtt) {
  chartTimeCounter++;
  chartTimeLabels.push(`${chartTimeCounter}s`);
  chartTxSpeedData.push(txMbps);
  chartRxSpeedData.push(rxMbps);
  chartRttData.push(rtt !== null ? rtt : (chartRttData.length > 0 ? chartRttData[chartRttData.length - 1] : 0));

  if (chartTimeLabels.length > maxChartPoints) {
    chartTimeLabels.shift();
    chartTxSpeedData.shift();
    chartRxSpeedData.shift();
    chartRttData.shift();
  }

  if (chart) chart.update('none');
}

// ============================================================
// Event Listeners
// ============================================================
document.getElementById('btn-create-room').addEventListener('click', () => {
  const randomRoom = Math.floor(10000 + Math.random() * 90000).toString();
  joinRoom(randomRoom);
});

document.getElementById('btn-join-room').addEventListener('click', () => {
  const roomInput = document.getElementById('room-id-input').value.trim();
  if (roomInput) {
    joinRoom(roomInput);
  } else {
    alert('Please enter a Room ID.');
  }
});

elBtnDisconnect.addEventListener('click', () => {
  resetUI();
  window.location.href = window.location.origin;
});

elFileInput.addEventListener('change', (e) => {
  if (e.target.files.length > 0) {
    selectedFile = e.target.files[0];
    elBtnStartSend.removeAttribute('disabled');
    log(`Selected file: ${selectedFile.name} (${formatBytes(selectedFile.size)})`);
  } else {
    selectedFile = null;
    elBtnStartSend.setAttribute('disabled', 'true');
  }
});

elBtnStartSend.addEventListener('click', () => {
  if (selectedFile && txChannel && txChannel.readyState === 'open') {
    startSendingFile();
  }
});

if (elBtnPauseSend) elBtnPauseSend.addEventListener('click', pauseTransfer);
if (elBtnResumeSend) elBtnResumeSend.addEventListener('click', resumeTransfer);
if (elBtnCancelSend) elBtnCancelSend.addEventListener('click', cancelSend);
if (elBtnCancelRecv) elBtnCancelRecv.addEventListener('click', cancelRecv);

speedLimitInput.addEventListener('input', (e) => {
  const val = parseInt(e.target.value);
  if (val === 0) {
    speedLimitVal.textContent = 'Unlimited';
    speedLimit = 0;
  } else {
    speedLimitVal.textContent = `${val} MB/s`;
    speedLimit = val * 1024 * 1024;
  }
});

if (elChunkSizeSelect) {
  elChunkSizeSelect.addEventListener('change', (e) => {
    chunkSize = parseInt(e.target.value);
    log(`Chunk size changed to: ${(chunkSize / 1024).toFixed(0)} KB`);
    updateProtocolAnalysis();
  });
}

// ============================================================
// Bootstrap
// ============================================================
connectWebSocket();
initChart();
