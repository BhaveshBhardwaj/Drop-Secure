# ⚡ AirDrop for the Internet — P2P File Transfer

A **browser-to-browser, zero-server file transfer tool** built with WebRTC DataChannels. Files are streamed directly between peers with no intermediary upload server. Your files never leave your machines until they land on the receiver's disk.

---

## 🚀 Quick Start

```bash
# 1. Install dependencies (first time only)
npm install

# 2. Start the server
node server.js

# 3. Open in browser
http://localhost:3000
```

> **To expose to the internet**, use [ngrok](https://ngrok.com/) or any tunnel:
> ```bash
> ngrok http 3000
> ```
> Share the ngrok HTTPS URL with the other person.

---

## 🏗 How It Works

### Architecture

```
[Browser A] ←── WebRTC DataChannel (direct P2P) ──→ [Browser B]
                         ↑
               WebSocket Signaling Only
                (node server.js — no file data passes through here)
```

1. **Signaling** — Both browsers connect to the Node.js WebSocket server (`server.js`) to exchange WebRTC offer/answer SDP and ICE candidates. This is the only traffic that touches your server.
2. **WebRTC DataChannel** — Once ICE negotiation completes, a **direct peer-to-peer connection** is established. All file data flows through this channel, bypassing the server entirely.
3. **Full Duplex** — Two separate DataChannels are created: `send-channel` (for A→B data) and `recv-channel` (for B→A data). Both peers can send and receive simultaneously.

### File Storage — No Temp Files

When the receiver accepts a file:

- **Chrome / Edge** (with File System Access API): `showSaveFilePicker()` opens a native save dialog. The browser writes chunks directly to the chosen file via a `FileSystemWritableFileStream`. You may briefly see a `.crswap` file — this is Chrome's internal atomic-write mechanism. It is **automatically renamed** to the final filename when the stream is closed. No intermediate copy is ever held in RAM.
- **Firefox / fallback**: Chunks are buffered in RAM (a `Blob`), and a "Save File" button appears when the transfer completes. Click it to trigger the browser's native download.

---

## ✨ Features

| Feature | Details |
|---|---|
| **Direct P2P Transfer** | No upload server — file data never touches `server.js` |
| **Full Duplex** | Both peers can send and receive simultaneously |
| **Pause / Resume** | Pause a running transfer mid-way; resume from the exact same byte position |
| **Cancel Transfer** | Sender or receiver can cancel at any time; `.crswap` temp file is cleaned up automatically |
| **Stall Detection** | If no data flows for 8 seconds, a visible warning is shown with a pulsing amber alert |
| **Chunk Pre-read Pipeline** | The next chunk is read from disk while the previous one is being sent, eliminating serialization delay |
| **Dynamic Chunk Size** | Choose 4 KB → 256 KB chunks in real time. Larger chunks = higher throughput on fast links |
| **Speed Throttling** | Optional upload speed cap (MB/s) using a slider |
| **Real-Time Chart** | Live dual-line chart showing TX speed (Mbps), RX speed (Mbps), and RTT (ms) simultaneously |
| **Real RTT** | Round-trip time from `RTCPeerConnection.getStats()` → `candidate-pair.currentRoundTripTime` |
| **Real Speed** | Speed is calculated from actual bytes transferred per second — no simulated values |
| **Auto-Reconnect** | WebSocket reconnects automatically if dropped; WebRTC session resets cleanly on peer disconnect |

---

## 🎛 Transmission Settings

### Chunk Size

Controls how much data is sent per WebRTC message. Affects throughput vs. latency:

| Chunk | Best For |
|---|---|
| 4–8 KB | Very lossy networks, low-power devices |
| **16 KB** | **Default — balanced for most conditions** |
| 32–64 KB | Fast LAN connections |
| 128–256 KB | Gigabit LAN or very stable high-speed links |

The app gives you real-time advice in the "Optimal Config Tip" box based on your measured RTT.

### Speed Limit

Set a maximum upload rate in MB/s. Useful if you want to share bandwidth during a transfer. Set to 0 for unlimited.

---

## ⏸ Pause / Resume

During an active upload:
- Click **⏸ Pause** to halt transmission. The receiver shows "Transfer paused".
- Click **▶ Resume** to continue from exactly where it stopped — no retransmission.
- Click **✕ Cancel** to abort. The receiver's `.crswap` temp file is cleaned up automatically.

The receiver can also click **✕ Cancel Transfer** at any time to abort, which notifies the sender.

---

## 📊 Live Chart

The bottom chart plots three real-time series every second:
- **TX Speed (Mbps)** — indigo line — upload speed on this peer
- **RX Speed (Mbps)** — emerald line — download speed on this peer
- **RTT (ms)** — purple dashed line — round-trip time from WebRTC stats

Both TX and RX are plotted simultaneously, giving a true picture of full-duplex utilization.

---

## 🔒 Security & Privacy

- Files are transferred **encrypted** via DTLS (the WebRTC data channel security standard).
- The signaling server (`server.js`) **never sees file contents** — it only routes ~1 KB of SDP/ICE messages per session.
- No analytics, no logging, no cloud storage.

---

## 🛠 Tech Stack

| Layer | Technology |
|---|---|
| Transport | WebRTC DataChannel (SCTP over DTLS) |
| Signaling | Node.js + `ws` WebSocket library |
| Frontend | Vanilla HTML/CSS/JS |
| UI Framework | None |
| Charts | Chart.js |
| Fonts | Outfit + JetBrains Mono (Google Fonts) |

---

## 📦 File Structure

```
data transfer/
├── server.js          # WebSocket signaling server (no file data)
├── package.json
└── public/
    ├── index.html     # UI layout
    ├── app.js         # All WebRTC logic, state, and event handling
    └── style.css      # Glassmorphism dark-mode design system
```

---

## ❓ FAQ

**Q: Is anything stored on the server?**  
A: No. `server.js` only relays ~1 KB of WebRTC handshake messages. Zero file data touches the server.

**Q: What is the `.crswap` file I see during transfer?**  
A: Chrome's File System Access API writes to a `.crswap` shadow file for safety. When `writer.close()` is called, Chrome atomically renames it to the real filename. It disappears automatically on success. If a transfer is cancelled, the app calls `writer.abort()` which deletes the `.crswap` file.

**Q: Can both peers send files at the same time?**  
A: Yes. There are two independent WebRTC DataChannels — one in each direction. Both can be active simultaneously.

**Q: What is the maximum file size?**  
A: There is no artificial limit. Files up to many gigabytes have been tested. The only limit is available disk space on the receiver's machine.

**Q: Is there any financial cost to run this?**  
A: No, beyond the cost of your own server (or laptop). The server only routes handshake messages (a few KB per session). TURN server costs only apply if direct P2P fails (symmetric NAT); with STUN only, there is no relay cost.
