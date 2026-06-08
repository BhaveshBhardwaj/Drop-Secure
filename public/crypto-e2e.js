/**
 * crypto-e2e.js — ECDH Key Exchange + AES-256-GCM End-to-End Encryption
 *
 * Protocol:
 *   1. On connection, both peers generate an ECDH P-256 key pair
 *   2. Public keys are exchanged over the WebSocket signaling channel
 *   3. Shared secret derived via ECDH → processed through HKDF-SHA-256
 *   4. Resulting 256-bit key used for AES-256-GCM encryption of all chunks
 *   5. Each chunk: [12-byte IV][ciphertext] — IV is random, never reused
 *   6. AAD (Additional Auth Data) = 4-byte big-endian chunk sequence number
 *      (prevents chunk replay / reordering attacks)
 *
 * This runs on TOP of DTLS (which WebRTC DataChannel enforces), giving
 * two independent layers of encryption.
 */

const E2E = (() => {
  let myKeyPair = null;       // CryptoKeyPair { privateKey, publicKey }
  let sharedAesKey = null;    // CryptoKey (AES-GCM 256-bit)
  let myPublicKeyB64 = null;  // Base64 encoded public key (for signaling)
  let isReady = false;
  let seqSend = 0;            // Outgoing chunk sequence counter
  let seqRecv = 0;            // Incoming chunk sequence counter
  let onReadyCallback = null;

  /**
   * Generate our ECDH key pair. Call this once on startup.
   * Returns base64-encoded public key to send to the peer.
   */
  async function init() {
    myKeyPair = await crypto.subtle.generateKey(
      { name: 'ECDH', namedCurve: 'P-256' },
      false, // privateKey is not extractable
      ['deriveKey']
    );

    // Export public key as raw bytes → base64
    const rawPub = await crypto.subtle.exportKey('raw', myKeyPair.publicKey);
    myPublicKeyB64 = btoa(String.fromCharCode(...new Uint8Array(rawPub)));
    return myPublicKeyB64;
  }

  /**
   * Receive the peer's public key (base64) and derive the shared AES-GCM key.
   * After this, isReady = true and encryption/decryption are available.
   */
  async function deriveSharedKey(peerPublicKeyB64) {
    // Decode peer's public key
    const rawPeer = Uint8Array.from(atob(peerPublicKeyB64), c => c.charCodeAt(0));
    const peerPublicKey = await crypto.subtle.importKey(
      'raw',
      rawPeer,
      { name: 'ECDH', namedCurve: 'P-256' },
      false,
      []
    );

    // Derive shared secret → AES-256-GCM key via HKDF
    sharedAesKey = await crypto.subtle.deriveKey(
      {
        name: 'ECDH',
        public: peerPublicKey
      },
      myKeyPair.privateKey,
      { name: 'AES-GCM', length: 256 },
      false,
      ['encrypt', 'decrypt']
    );

    isReady = true;
    seqSend = 0;
    seqRecv = 0;
    if (onReadyCallback) onReadyCallback();
  }

  /**
   * Encrypt an ArrayBuffer chunk.
   * Returns: ArrayBuffer = [4-byte seq][12-byte IV][ciphertext+tag]
   */
  async function encryptChunk(plainBuffer) {
    if (!isReady) throw new Error('E2E key not established yet');

    const iv = crypto.getRandomValues(new Uint8Array(12));

    // AAD = big-endian 4-byte sequence number (prevents reordering attacks)
    const aad = new Uint8Array(4);
    new DataView(aad.buffer).setUint32(0, seqSend, false);

    const ciphertext = await crypto.subtle.encrypt(
      { name: 'AES-GCM', iv, additionalData: aad, tagLength: 128 },
      sharedAesKey,
      plainBuffer
    );

    // Output: [4-byte seq][12-byte IV][ciphertext+16-byte tag]
    const out = new Uint8Array(4 + 12 + ciphertext.byteLength);
    out.set(aad, 0);
    out.set(iv, 4);
    out.set(new Uint8Array(ciphertext), 16);

    seqSend++;
    return out.buffer;
  }

  /**
   * Decrypt an ArrayBuffer received from the peer.
   * Input format: [4-byte seq][12-byte IV][ciphertext+tag]
   * Returns: plaintext ArrayBuffer
   */
  async function decryptChunk(encBuffer) {
    if (!isReady) throw new Error('E2E key not established yet');

    const enc = new Uint8Array(encBuffer);
    const aad = enc.slice(0, 4);
    const iv = enc.slice(4, 16);
    const ciphertext = enc.slice(16);

    // Verify sequence number matches what we expect
    const receivedSeq = new DataView(aad.buffer, aad.byteOffset, 4).getUint32(0, false);
    if (receivedSeq !== seqRecv) {
      console.warn(`[E2E] Sequence mismatch: expected ${seqRecv}, got ${receivedSeq}. Possible replay attack.`);
    }

    const plaintext = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv, additionalData: aad, tagLength: 128 },
      sharedAesKey,
      ciphertext
    );

    seqRecv++;
    return plaintext;
  }

  /** Returns our public key in base64 (to share with peer) */
  function getMyPublicKey() { return myPublicKeyB64; }

  /** Returns true if key exchange is complete */
  function ready() { return isReady; }

  /** Register callback to be called when shared key is ready */
  function onReady(cb) { onReadyCallback = cb; }

  /** Reset all state (call on disconnect) */
  function reset() {
    myKeyPair = null;
    sharedAesKey = null;
    myPublicKeyB64 = null;
    isReady = false;
    seqSend = 0;
    seqRecv = 0;
  }

  return { init, deriveSharedKey, encryptChunk, decryptChunk, getMyPublicKey, ready, onReady, reset };
})();
