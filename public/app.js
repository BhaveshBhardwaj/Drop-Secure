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
let filesToSend = []; // Array of { file: File, relativePath: string }
let currentFileIndex = 0;
let uploadOffset = 0;
let uploadStartTime = 0;
let uploadLastBytes = 0;
let uploadLastTime = 0;
let isUploading = false;
let isPaused = false;
let lastChunkTime = 0;
let totalUploadSize = 0;
let totalUploadedBytes = 0;

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

// ── Receiver incoming buffer queue (prevent out-of-order decryption/processing) ──
let rxQueue = [];
let isProcessingRx = false;

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
const elBtnSelectFile = document.getElementById('btn-select-file');
const elBtnSelectFolder = document.getElementById('btn-select-folder');
const elSelectedFilesList = document.getElementById('selected-files-list');
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

// Chat UI
const elChatInput = document.getElementById('chat-input');
const elBtnSendChat = document.getElementById('btn-send-chat');
const elChatMessages = document.getElementById('chat-messages');

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
      elPeerRoleText.textContent = 'Peer Connected. Ready to transfer and chat.';
      if (elBtnSelectFile) elBtnSelectFile.disabled = false;
      if (elBtnSelectFolder) elBtnSelectFolder.disabled = false;
      
      // Enable chat
      if (elChatInput) elChatInput.disabled = false;
      if (elBtnSendChat) elBtnSendChat.disabled = false;
      if (elChatMessages) elChatMessages.innerHTML = '';
      appendChatMessage('System', 'Secure E2E connection established. You can now chat.', 'system');

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
  if (elBtnSelectFile) elBtnSelectFile.disabled = true;
  if (elBtnSelectFolder) elBtnSelectFolder.disabled = true;
  
  // Disable chat
  if (elChatInput) elChatInput.disabled = true;
  if (elBtnSendChat) elBtnSendChat.disabled = true;

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
    if (elBtnSelectFile) elBtnSelectFile.disabled = false;
    if (elBtnSelectFolder) elBtnSelectFolder.disabled = false;
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
    } else if (msg.type === 'chat') {
      appendChatMessage('Peer', msg.text, 'received');
    }
  };
}

// ============================================================
// Receive Channel Setup
// ============================================================
let incomingFiles = [];
let fileHandleMap = new Map();
let totalReceivedBytes = 0;

function setupRxChannel(channel) {
  channel.binaryType = 'arraybuffer';

  channel.onopen = () => log('Receive data channel opened.');
  channel.onclose = () => log('Receive data channel closed.');

  channel.onmessage = async (event) => {
    if (typeof event.data === 'string') {
      const msg = JSON.parse(event.data);

      if (msg.type === 'meta') {
        incomingFiles = msg.files;
        downloadFileSize = msg.totalSize;

        log(`Incoming offer: ${incomingFiles.length} files (${formatBytes(downloadFileSize)})`);

        elIncomingFilename.textContent = incomingFiles.length > 1 ? `${incomingFiles.length} files / folders` : incomingFiles[0].name;
        elIncomingFilesize.textContent = formatBytes(downloadFileSize);

        elDownloadStatsPanel.classList.add('hidden');
        if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.add('hidden');
        downloadedBlob = null;

        elNoIncomingPrompt.classList.add('hidden');
        elIncomingFileAlert.classList.remove('hidden');

        elBtnAcceptFile.onclick = async () => {
          elIncomingFileAlert.classList.add('hidden');
          elDownloadStatsPanel.classList.remove('hidden');
          if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.add('hidden');
          elRxStatusText.textContent = 'Preparing folders...';
          elRxIntegrityStatus.textContent = '';

          try {
            if ('showDirectoryPicker' in window && incomingFiles.length > 1) {
              const dirHandle = await window.showDirectoryPicker({ mode: 'readwrite' });
              for (let i = 0; i < incomingFiles.length; i++) {
                const parts = incomingFiles[i].name.split('/');
                let currDir = dirHandle;
                for (let j = 0; j < parts.length - 1; j++) {
                  currDir = await currDir.getDirectoryHandle(parts[j], { create: true });
                }
                const fileHandle = await currDir.getFileHandle(parts[parts.length - 1], { create: true });
                fileHandleMap.set(i, fileHandle);
              }
              log('Folders and file handles created successfully.');
            } else if ('showSaveFilePicker' in window && incomingFiles.length === 1) {
              const fileHandle = await window.showSaveFilePicker({ suggestedName: incomingFiles[0].name });
              fileHandleMap.set(0, fileHandle);
            } else {
              log('File System Access API not fully supported or user denied. Using memory fallback (browser may crash on huge files).');
            }

            totalReceivedBytes = 0;
            isDownloading = true;
            downloadStartTime = performance.now();
            downloadLastTime = downloadStartTime;
            downloadLastBytes = 0;
            rxLastProgressBytes = 0;
            rxQueue = [];
            isProcessingRx = false;

            if (rxChannel && rxChannel.readyState === 'open') {
              rxChannel.send(JSON.stringify({ type: 'ready' }));
            }
            elRxStatusText.textContent = 'Receiving...';

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

      } else if (msg.type === 'cancel') {
        log('Sender cancelled the transfer.');
        stopRxStallTimer();
        resetDownloadUI();
        elRxStatusText.textContent = 'Cancelled by sender';
      } else if (msg.type === 'chat') {
        appendChatMessage('Peer', msg.text, 'received');
      } else if (['file_start', 'file_done', 'done'].includes(msg.type)) {
        rxQueue.push({ type: 'cmd', msg });
        processRxQueue();
      }
    } else {
      // Binary chunk
      rxQueue.push({ type: 'data', chunk: event.data });
      processRxQueue();
    }
  };
}

// ============================================================
// Receiver Queue Processor
// ============================================================
async function processRxQueue() {
  if (isProcessingRx || rxQueue.length === 0) return;
  isProcessingRx = true;

  while (rxQueue.length > 0) {
    const item = rxQueue.shift();

    if (item.type === 'cmd') {
      const msg = item.msg;
      if (msg.type === 'file_start') {
        log(`Starting to receive file ${msg.index}`);
        if (fileHandleMap.has(msg.index)) {
          fileWriter = await fileHandleMap.get(msg.index).createWritable();
        } else {
          fileWriter = []; // fallback memory array
        }
      } else if (msg.type === 'file_done') {
        log(`Finished receiving file ${msg.index}`);
        if (fileWriter && typeof fileWriter.write === 'function') {
          writeQueue.push({ type: 'CMD', cmd: 'CLOSE_FILE' });
          processWriteQueue();
        } else if (Array.isArray(fileWriter)) {
          // Memory fallback zip or download logic could go here if needed.
          // For now, if it's 1 file, we download it at the very end ('done')
        }
      } else if (msg.type === 'done') {
        log('All files received successfully.');
        isDownloading = false;
        stopRxStallTimer();
        elRxStatusText.textContent = 'Saved ✓';
        if (Array.isArray(fileWriter) && incomingFiles.length === 1) {
           triggerFallbackDownload(incomingFiles[0].name, fileWriter);
        }
      }
    } else if (item.type === 'data') {
      let decChunk;
      if (window.E2E && E2E.ready()) {
        try {
          decChunk = await E2E.decryptChunk(item.chunk);
        } catch (err) {
          log(`❌ Decryption error: ${err.message}`);
          cancelRecv();
          isProcessingRx = false;
          return;
        }
      } else {
        cancelRecv();
        isProcessingRx = false;
        return;
      }

      totalReceivedBytes += decChunk.byteLength;
      
      if (fileWriter && typeof fileWriter.write === 'function') {
        writeQueue.push(decChunk);
        processWriteQueue();
      } else if (Array.isArray(fileWriter)) {
        fileWriter.push(decChunk);
      }

      resetRxStallTimer();
      const pct = ((totalReceivedBytes / downloadFileSize) * 100).toFixed(1);
      elRxProgressBar.style.width = `${pct}%`;
      elRxProgressPercent.textContent = `${pct}%`;
    }
  }

  isProcessingRx = false;
}

// ============================================================
// Write Queue
// ============================================================
async function processWriteQueue() {
  if (isWriting || writeQueue.length === 0) return;
  isWriting = true;

  while (writeQueue.length > 0) {
    const item = writeQueue.shift();
    try {
      if (item.type === 'CMD' && item.cmd === 'CLOSE_FILE') {
        if (fileWriter) {
          await fileWriter.close();
          fileWriter = null;
        }
      } else if (fileWriter && typeof fileWriter.write === 'function') {
        await fileWriter.write(item);
      }
    } catch (e) {
      log(`Disk write error: ${e.message}`);
      isWriting = false;
      if (rxChannel && rxChannel.readyState === 'open') {
        rxChannel.send(JSON.stringify({ type: 'cancel' }));
      }
      resetDownloadUI();
      return;
    }
  }

  isWriting = false;
}

function triggerFallbackDownload(filename, chunksArray) {
  log('Assembling in-memory download...');
  try {
    downloadedBlob = new Blob(chunksArray);
    elRxStatusText.textContent = 'Ready to Save';
    elRxIntegrityStatus.textContent = 'Transfer complete. Click below to save.';
    if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.remove('hidden');
    if (elBtnFallbackDownload) {
      elBtnFallbackDownload.onclick = () => {
        const url = URL.createObjectURL(downloadedBlob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        a.click();
        URL.revokeObjectURL(url);
        if (elFallbackDownloadContainer) elFallbackDownloadContainer.classList.add('hidden');
      };
    }
  } catch (e) {
    log(`Error assembling in-memory file: ${e.message}`);
  }
}

// ============================================================
// Sending: Offer, Stream, Pause/Resume, Cancel
// ============================================================
function startSendingFiles() {
  if (window.E2E && !E2E.ready()) {
    alert('End-to-end secure channel is still establishing. Please wait a moment.');
    return;
  }
  
  log(`Offering to send ${filesToSend.length} file(s) (Total: ${formatBytes(totalUploadSize)})`);
  
  const metaFiles = filesToSend.map(f => ({ name: f.relativePath, size: f.file.size }));
  
  txChannel.send(JSON.stringify({
    type: 'meta',
    files: metaFiles,
    totalSize: totalUploadSize
  }));
  
  elUploadStatsPanel.classList.remove('hidden');
  elTxStatusText.textContent = 'Waiting for peer approval...';
  elBtnPauseSend.style.display = '';
  elBtnResumeSend.style.display = 'none';
}

async function startStreaming() {
  currentFileIndex = 0;
  uploadOffset = 0;
  totalUploadedBytes = 0;
  isUploading = true;
  isPaused = false;
  uploadStartTime = performance.now();
  uploadLastTime = uploadStartTime;
  uploadLastBytes = 0;
  txLastProgressBytes = 0;
  chunkSize = 16384; // Reset chunk size on start

  elTxStatusText.textContent = 'Transmitting...';
  elBtnPauseSend.style.display = '';
  elBtnResumeSend.style.display = 'none';

  startStatsInterval();
  startTxStallTimer();

  sendNextChunks();
}

async function sendNextChunks() {
  if (!isUploading || isPaused) return;

  try {
    while (currentFileIndex < filesToSend.length) {
      const currentFileObj = filesToSend[currentFileIndex];
      const selectedFile = currentFileObj.file;
      
      // If just starting this file, send file_start
      if (uploadOffset === 0) {
        log(`Starting file ${currentFileIndex + 1}/${filesToSend.length}: ${currentFileObj.relativePath}`);
        txChannel.send(JSON.stringify({ type: 'file_start', index: currentFileIndex }));
        // Wait a tiny bit for the receiver to process the start message and open the writer
        await new Promise(r => setTimeout(r, 50));
      }

      while (uploadOffset < selectedFile.size) {
        if (!isUploading || isPaused) return;

        // Auto-Tuning Congestion Control (AIMD Algorithm)
        const buffered = txChannel.bufferedAmount;
        
        if (buffered > MAX_BUFFERED_AMOUNT) {
          // Buffer full: Multiplicative decrease & wait
          chunkSize = Math.max(4096, Math.floor(chunkSize * 0.75));
          await new Promise(resolve => setTimeout(resolve, 50));
          continue;
        } else if (buffered > MAX_BUFFERED_AMOUNT / 2 || lastRtt > 150) {
          // Moderate congestion: slight decrease
          chunkSize = Math.max(4096, Math.floor(chunkSize * 0.9));
        } else if (buffered < MAX_BUFFERED_AMOUNT / 10 && lastRtt < 50) {
          // Network clear: Additive increase
          chunkSize = Math.min(262144, chunkSize + 4096);
        }

        // Read current chunk directly from disk
        const chunk = selectedFile.slice(uploadOffset, uploadOffset + chunkSize);
        const buffer = await chunk.arrayBuffer();

        let encryptedBuffer = buffer;
        if (window.E2E && E2E.ready()) {
          encryptedBuffer = await E2E.encryptChunk(buffer);
        } else {
          log('⚠️ E2E encryption is not ready. Aborting transfer.');
          cancelSend();
          return;
        }

        txChannel.send(encryptedBuffer);
        uploadOffset += buffer.byteLength;
        totalUploadedBytes += buffer.byteLength;

        // Update progress UI using totalUploadedBytes
        const pct = ((totalUploadedBytes / totalUploadSize) * 100).toFixed(1);
        elTxProgressBar.style.width = `${pct}%`;
        elTxProgressPercent.textContent = `${pct}%`;

        // Reset stall timer — we just sent data
        resetTxStallTimer();
      }

      // File complete
      txChannel.send(JSON.stringify({ type: 'file_done', index: currentFileIndex }));
      log(`Finished sending ${currentFileObj.relativePath}`);
      
      currentFileIndex++;
      uploadOffset = 0;
      await new Promise(r => setTimeout(r, 50)); // Tiny yield between files
    }

    // All files complete
    txChannel.send(JSON.stringify({ type: 'done' }));
    log('Upload complete. Sent "done" signal to receiver.');
    elTxStatusText.textContent = 'Done ✓';
    isUploading = false;
    isPaused = false;
    stopTxStallTimer();
    elBtnPauseSend.style.display = 'none';
    elBtnResumeSend.style.display = 'none';
    
  } catch (err) {
    log(`❌ Transmission error: ${err.message}`);
    cancelSend();
  }
}

function pauseTransfer() {
  if (!isUploading || isPaused) return;
  isPaused = true;
  elTxStatusText.textContent = 'Paused';
  elBtnPauseSend.style.display = 'none';
  elBtnResumeSend.style.display = '';
  stopTxStallTimer();
  log('Transfer paused.');
}

function resumeTransfer() {
  if (!isUploading || !isPaused) return;
  isPaused = false;
  elTxStatusText.textContent = 'Transmitting...';
  elBtnPauseSend.style.display = '';
  elBtnResumeSend.style.display = 'none';
  txLastProgressBytes = totalUploadedBytes;
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
  rxQueue = [];
  isProcessingRx = false;

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
  if (typeof elProtocolAnalysis === 'undefined' || !elProtocolAnalysis) return;
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
  console.log(msg);
  if (typeof elConsoleLogs !== 'undefined' && elConsoleLogs) {
    const time = new Date().toLocaleTimeString();
    elConsoleLogs.textContent += `\n[${time}] ${msg}`;
    elConsoleLogs.scrollTop = elConsoleLogs.scrollHeight;
  }
}

function clearLogs() {
  if (typeof elConsoleLogs !== 'undefined' && elConsoleLogs) {
    elConsoleLogs.textContent = 'Logs cleared.';
  }
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
// Chat Logic
// ============================================================
function appendChatMessage(sender, text, type) {
  if (!elChatMessages) return;
  const msgDiv = document.createElement('div');
  msgDiv.className = `chat-msg ${type}`;
  
  const textSpan = document.createElement('span');
  textSpan.textContent = text;
  msgDiv.appendChild(textSpan);
  
  const timeSpan = document.createElement('span');
  timeSpan.className = 'time';
  timeSpan.textContent = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  msgDiv.appendChild(timeSpan);
  
  elChatMessages.appendChild(msgDiv);
  elChatMessages.scrollTop = elChatMessages.scrollHeight;
}

function sendChatMessage() {
  const text = elChatInput.value.trim();
  if (!text) return;
  
  if (txChannel && txChannel.readyState === 'open') {
    txChannel.send(JSON.stringify({ type: 'chat', text }));
    appendChatMessage('You', text, 'sent');
    elChatInput.value = '';
  } else {
    log('Cannot send message: no active connection.');
  }
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

async function scanDirectory(dirHandle, path = '') {
  let files = [];
  for await (const entry of dirHandle.values()) {
    if (entry.kind === 'file') {
      const file = await entry.getFile();
      files.push({ handle: entry, file: file, relativePath: `${path}${file.name}` });
    } else if (entry.kind === 'directory') {
      const subFiles = await scanDirectory(entry, `${path}${entry.name}/`);
      files.push(...subFiles);
    }
  }
  return files;
}

function updateSelectedFilesUI() {
  if (filesToSend.length === 0) {
    elSelectedFilesList.innerHTML = 'No content selected.';
    elBtnStartSend.setAttribute('disabled', 'true');
    return;
  }
  
  totalUploadSize = filesToSend.reduce((acc, f) => acc + f.file.size, 0);
  
  if (filesToSend.length === 1) {
    elSelectedFilesList.innerHTML = `<strong>${filesToSend[0].relativePath}</strong> (${formatBytes(totalUploadSize)})`;
  } else {
    elSelectedFilesList.innerHTML = `<strong>${filesToSend.length} files</strong> selected (${formatBytes(totalUploadSize)})<br>` +
      filesToSend.slice(0, 3).map(f => `<small>${f.relativePath}</small>`).join('<br>') +
      (filesToSend.length > 3 ? `<br><small>...and ${filesToSend.length - 3} more</small>` : '');
  }
  elBtnStartSend.removeAttribute('disabled');
}

if (elBtnSelectFile) {
  elBtnSelectFile.addEventListener('click', async () => {
    try {
      const handles = await window.showOpenFilePicker({ multiple: true });
      filesToSend = [];
      for (const handle of handles) {
        const file = await handle.getFile();
        filesToSend.push({ handle, file, relativePath: file.name });
      }
      log(`Selected ${filesToSend.length} file(s).`);
      updateSelectedFilesUI();
    } catch (e) {
      log('File selection cancelled or failed.');
    }
  });
}

if (elBtnSelectFolder) {
  elBtnSelectFolder.addEventListener('click', async () => {
    try {
      const dirHandle = await window.showDirectoryPicker();
      log('Scanning folder, please wait...');
      elSelectedFilesList.innerHTML = 'Scanning folder...';
      filesToSend = await scanDirectory(dirHandle, `${dirHandle.name}/`);
      log(`Scanned folder. Found ${filesToSend.length} file(s).`);
      updateSelectedFilesUI();
    } catch (e) {
      log('Folder selection cancelled or failed.');
      updateSelectedFilesUI();
    }
  });
}

if (elBtnStartSend) {
  elBtnStartSend.addEventListener('click', () => {
    if (filesToSend.length > 0 && txChannel && txChannel.readyState === 'open') {
      startSendingFiles();
    }
  });
}

if (elBtnPauseSend) elBtnPauseSend.addEventListener('click', pauseTransfer);
if (elBtnResumeSend) elBtnResumeSend.addEventListener('click', resumeTransfer);
if (elBtnCancelSend) elBtnCancelSend.addEventListener('click', cancelSend);
if (elBtnCancelRecv) elBtnCancelRecv.addEventListener('click', cancelRecv);

if (elBtnSendChat) {
  elBtnSendChat.addEventListener('click', sendChatMessage);
}

if (elChatInput) {
  elChatInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendChatMessage();
    }
  });
}

// ============================================================
// Bootstrap
// ============================================================
connectWebSocket();
initChart();
