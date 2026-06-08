# ⚡ DropSecure — Secure SaaS P2P File Transfer Platform

DropSecure is a premium, **browser-to-browser, zero-server-storage file transfer SaaS platform** built with WebRTC DataChannels and enhanced with an application-layer **End-to-End Encryption (E2E)** protocol. Files are streamed directly between peers without passing through intermediate servers.

---

## 🚀 Quick Start

### 1. Prerequisites
- Node.js (v16+)
- MongoDB Atlas database (or a local MongoDB instance)

### 2. Installation
```bash
# Clone the repository
git clone https://github.com/BhaveshBhardwaj/Drop-Secure.git
cd Drop-Secure

# Install dependencies
npm install
```

### 3. Environment Setup
Create a `.env` file in the root directory:
```env
MONGO_URI=mongodb+srv://your_connection_string
JWT_SECRET=your_jwt_signing_secret_min_64_characters
PORT=3000
```

### 4. Running the Application
```bash
# Start the server
npm start
```
Open your browser and navigate to `http://localhost:3000`.

---

## 🏗️ Architecture

```
                       Signaling & Authentication
                       [ Node.js + WebSocket Server ]
                               /            \
                       HTTPS  /              \  HTTPS
                             /                \
                     [ Browser A ] <────────> [ Browser B ]
                                  Direct P2P Link
                               (WebRTC DataChannel)
```

1. **User Authentication** — Users register and log in via secure endpoints (`/api/auth/register` and `/api/auth/login`) using **bcryptjs** password hashing and **JSON Web Tokens (JWT)**.
2. **Signaling** — Authorized browsers connect to the server's WebSocket to exchange WebRTC offer/answer SDPs and ICE candidates. No file data is ever handled by the socket server.
3. **WebRTC Data Channel** — A direct, peer-to-peer data connection is established between browsers using DTLS transport encryption.
4. **Application Layer Encryption** — An independent client-side cryptographic layer is established directly between the browsers using ECDH and AES-GCM.

---

## 🔒 Security & Privacy Protocol

DropSecure features a **double layer of encryption** to ensure maximum privacy:

*   **Layer 1: WebRTC DTLS** — Automatic transport-layer encryption enforced by the WebRTC protocol.
*   **Layer 2: ECDH + AES-256-GCM E2E** — Application-layer cryptography implemented via the native browser **Web Crypto API**:
    1.  Upon connection, each peer generates an ephemeral ECDH (P-256 curve) key pair.
    2.  Public keys are exchanged via WebSocket signaling.
    3.  A 256-bit AES shared key is derived client-side via HKDF-SHA-256.
    4.  Every binary chunk is encrypted with `AES-256-GCM` using a unique, random 12-byte IV and signed with a 4-byte sequence number as **Additional Authenticated Data (AAD)** to prevent packet replay or out-of-order injection attacks.

---

## ✨ SaaS Features

*   **Premium Landing Page** — Beautiful design with moving gradients, floating particles, interactive feature panels, pricing tables, and CTA flows.
*   **Secure Auth Guards** — User registration with a live password strength meter and confirmation validator. Route guarding redirects unauthorized users back to login while preserving room sharing params (`?room=xxxxx`) in the URL query string.
*   **No Intermediary Storage** — Data is written directly to the receiver's disk using Chrome's **File System Access API** (`showSaveFilePicker`). Memory buffering is used as a fallback on unsupported browsers.
*   **Pause & Resume** — Stop transfers mid-way and resume from the exact byte offset without retransmitting data.
*   **Stall Watchdog** — Automatically triggers warnings if no data packet is received or sent for 8 seconds.
*   **Throttling & Performance Control** — Set bandwidth caps (MB/s) and adjust chunk sizes (4 KB to 256 KB) dynamically during live transmissions.
*   **Live Metrics Chart** — A dual-line real-time Chart.js graph tracking upload (TX) speed, download (RX) speed, and round-trip time (RTT).

---

## 📦 File Structure

```
Drop-Secure/
├── server.js          # Node.js backend (Express, Mongoose, WebSocket signaling)
├── package.json       # Node dependency configurations
├── .gitignore         # Version control exclusion file
├── .env               # Configuration parameters (ignored by git)
└── public/
    ├── index.html     # SaaS public landing page
    ├── login.html     # User login interface
    ├── signup.html    # User registration interface
    ├── app.html       # Protected dashboard interface
    ├── app.js         # Client-side WebRTC logic and event handlers
    ├── crypto-e2e.js  # ECDH + AES-256-GCM browser cryptographic routines
    ├── style.css      # Shared dark-mode UI stylesheet
    ├── landing.css    # Landing-page animations and visuals
    └── auth.css       # Form glassmorphism stylesheet
```
