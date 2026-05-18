// hydrotix-bridge/bridge.js
require('dotenv').config(); // load dari file .env secara otomatis
const mqtt  = require('mqtt');
const admin = require('firebase-admin');

// ── Firebase Admin — pakai env variables (aman untuk Git) ──────────
admin.initializeApp({
  credential: admin.credential.cert({
    projectId:   process.env.FIREBASE_PROJECT_ID,
    clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
    privateKey:  process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n'),
  }),
});

const db = admin.firestore();
console.log('[FIREBASE] Admin SDK OK');

// ── MQTT CONFIG ────────────────────────────────────────────────────
const client = mqtt.connect('mqtts://f09444b946fd4a51b72b11e2d900d11d.s1.eu.hivemq.cloud', {
  port:            8883,
  username:        'admin_hydrotix',
  password:        'qesSuz-6berpa-junmez',
  clientId:        `bridge_render_${Math.random().toString(16).substring(2, 8)}`,
  reconnectPeriod: 5000,
  keepalive:       60,
});

// ── CONNECT ────────────────────────────────────────────────────────
client.on('connect', () => {
  console.log('[BRIDGE] ✅ Terhubung ke HiveMQ!');

  client.subscribe('hydro/+/+/telemetry', { qos: 1 }, (err) => {
    if (!err) console.log('[BRIDGE] Subscribe: hydro/+/+/telemetry');
    else      console.error('[BRIDGE] Subscribe error:', err.message);
  });

  client.subscribe('hydro/+/+/heartbeat', { qos: 1 }, (err) => {
    if (!err) console.log('[BRIDGE] Subscribe: hydro/+/+/heartbeat');
    else      console.error('[BRIDGE] Subscribe error:', err.message);
  });

  client.subscribe('hydro/setup/+/request', { qos: 1 }, (err) => {
    if (!err) console.log('[BRIDGE] Subscribe: hydro/setup/+/request (ZTP)');
    else      console.error('[BRIDGE] Subscribe error:', err.message);
  });
});

client.on('reconnect', () => console.log('[BRIDGE] Reconnecting...'));
client.on('error',     (err) => console.error('[BRIDGE] Error:', err.message));
client.on('offline',   () => console.log('[BRIDGE] Offline'));

// ── MESSAGE HANDLER ────────────────────────────────────────────────
client.on('message', async (topic, message) => {
  const parts = topic.split('/');
  
  // Deteksi ZTP Request
  if (parts.length === 4 && parts[0] === 'hydro' && parts[1] === 'setup' && parts[3] === 'request') {
    const macAddress = parts[2];
    await handleZtpRequest(macAddress);
    return;
  }

  if (parts.length !== 4 || parts[0] !== 'hydro') return;

  const [, userId, deviceId, type] = parts;

  let data;
  try {
    data = JSON.parse(message.toString());
  } catch (e) {
    console.error(`[BRIDGE] JSON parse error di ${topic}:`, e.message);
    return;
  }

  if (type === 'telemetry') await handleTelemetry(userId, deviceId, data);
  if (type === 'heartbeat') await handleHeartbeat(userId, deviceId, data); 
});

// ── HANDLER ZTP (ZERO TOUCH PROVISIONING) ──────────────────────────
async function handleZtpRequest(macAddress) {
  console.log(`\n[ZTP] 🔍 Menerima permintaan setup dari MAC: ${macAddress}`);
  try {
    const devicesRef = db.collection('devices');
    const snapshot = await devicesRef.where('macAddress', '==', macAddress).limit(1).get();

    if (snapshot.empty) {
      console.log(`[ZTP] ❌ Device dengan MAC ${macAddress} belum didaftarkan di Web Admin.`);
      return;
    }

    const doc = snapshot.docs[0];
    const data = doc.data();

    if (!data.ownerId) {
      console.log(`[ZTP] ❌ Device ditemukan tapi ownerId kosong.`);
      return;
    }

    const payload = JSON.stringify({
      ownerId: data.ownerId,
      deviceId: doc.id
    });

    const topicResponse = `hydro/setup/${macAddress}/response`;
    client.publish(topicResponse, payload, { qos: 1 });
    
    console.log(`[ZTP] ✅ Berhasil mengirim profil ke ESP32: ${topicResponse} -> ${payload}`);

  } catch (err) {
    console.error(`[ERROR] ZTP ${macAddress}:`, err.message);
  }
}

// ── HANDLER TELEMETRY ──────────────────────────────────────────────
async function handleTelemetry(userId, deviceId, data) {
  console.log(`\n[TELEMETRY] device: ${deviceId} | topic_user: ${userId}`);
  const now = admin.firestore.FieldValue.serverTimestamp();

  try {
    const deviceRef = db.collection('devices').doc(deviceId);
    const deviceSnap = await deviceRef.get();

    // CLOUD SELF-DESTRUCT SIGNAL (Jika alat sudah dihapus di web admin)
    if (!deviceSnap.exists) {
      console.log(`[WARNING] Device ${deviceId} telah dihapus dari sistem. Mengirim sinyal Factory Reset ke ESP32!`);
      
      const killTopic = `hydro/${userId}/${deviceId}/command`;
      const killPayload = JSON.stringify({ target: "factory_reset", state: true });
      
      client.publish(killTopic, killPayload, { qos: 1 });
      return;
    }

    const validOwnerId = deviceSnap.data().ownerId;

    // 1. Simpan ke devices/{deviceId}/readings
    const ref = await deviceRef
      .collection('readings')
      .add({
        timestamp:   now,
        ph:          data.ph          ?? null,
        tds:         data.tds         ?? null,
        waterTemp:   data.waterTemp   ?? null,
        airTemp:     data.airTemp     ?? null,
        humidity:    data.humidity    ?? null,
        waterLevel:  data.waterLevel  ?? null,
        do:          data.do          ?? null,
        alert:       data.alert       ?? false,
        relayStates: data.relayStates ?? {},
      });
    console.log(`[FIRESTORE] ✅ Reading tersimpan → ${ref.id}`);

    // 2. Update dokumen utama device
    await deviceRef.set({
      status:   'online',
      isOnline: true,
      lastSeen: now,
      lastReading: {
        ph:         data.ph         ?? null,
        tds:        data.tds        ?? null,
        waterTemp:  data.waterTemp  ?? null,
        airTemp:    data.airTemp    ?? null,
        humidity:   data.humidity   ?? null,
        waterLevel: data.waterLevel ?? null,
        do:         data.do         ?? null,
        alert:      data.alert      ?? false,
        updatedAt:  now,
      },
    }, { merge: true });
    console.log(`[FIRESTORE] ✅ Device ${deviceId} → status: online`);

    // 3. Simpan ke koleksi alerts jika ada anomali
    if (data.alert === true) {
      await db.collection('alerts').add({
        deviceId:  deviceId,
        userId:    validOwnerId,
        parameter: 'sensor_anomaly',
        severity:  'warning',
        value:     data.waterTemp ?? 0,
        message:   `Anomali sensor pada ${deviceId}`,
        status:    'unread',
        createdAt: now,
      });
      console.log(`[ALERT] ⚠️  Alert tersimpan untuk ${deviceId}`);
    }

    // 4. Catat ke logs
    await db.collection('logs').add({
      type:      'telemetry',
      deviceId:  deviceId,
      userId:    validOwnerId,
      note:      `Telemetry diterima dari ${deviceId}`,
      currentPh:   data.ph         ?? 0,
      currentTds:  data.tds        ?? 0,
      currentTemp: data.waterTemp  ?? 0,
      createdAt: now,
    });

  } catch (err) {
    console.error(`[ERROR] Telemetry ${deviceId}:`, err.message);
  }
}

// ── HANDLER HEARTBEAT ──────────────────────────────────────────────
async function handleHeartbeat(userId, deviceId, data = {}) {
  console.log(`[HEARTBEAT] 💓 ${deviceId} | MAC: ${data.macAddress || '-'} | IP: ${data.ip || '-'}`);
  try {
    const deviceRef = db.collection('devices').doc(deviceId);
    const deviceSnap = await deviceRef.get();

    // CLOUD SELF-DESTRUCT SIGNAL (Jika alat sudah dihapus di web admin)
    if (!deviceSnap.exists) {
      console.log(`[WARNING] Heartbeat dari device ${deviceId} yang telah dihapus. Mengirim sinyal Factory Reset ke ESP32!`);
      
      const killTopic = `hydro/${userId}/${deviceId}/command`;
      const killPayload = JSON.stringify({ target: "factory_reset", state: true });
      
      client.publish(killTopic, killPayload, { qos: 1 });
      return;
    }

    const update = {
      status:   'online',
      isOnline: true,
      lastSeen: admin.firestore.FieldValue.serverTimestamp(),
    };
    if (data.macAddress) update.macAddress = data.macAddress;
    if (data.ip)         update.ip         = data.ip;
    if (data.firmware)   update.firmwareVersion = data.firmware;

    await deviceRef.set(update, { merge: true });
  } catch (err) {
    console.error(`[ERROR] Heartbeat ${deviceId}:`, err.message);
  }
}

// ── DETEKSI DEVICE OFFLINE ─────────────────────────────────────────
async function checkOfflineDevices() {
  const cutoff = new Date(Date.now() - 5 * 60 * 1000);
  try {
    const snap = await db.collection('devices')
      .where('isOnline', '==', true)
      .where('lastSeen', '<', cutoff)
      .get();

    if (snap.empty) return;

    const batch = db.batch();
    snap.forEach(doc => {
      console.log(`[OFFLINE] 🔴 ${doc.id} → offline`);
      batch.update(doc.ref, { isOnline: false, status: 'offline' });
    });
    await batch.commit();
  } catch (err) {
    console.error('[ERROR] checkOfflineDevices:', err.message);
  }
}

setInterval(checkOfflineDevices, 5 * 60 * 1000);

// ── KEEP ALIVE log ─────────────────────────────────────────────────
setInterval(() => {
  console.log(`[PING] 🟢 Bridge aktif — ${new Date().toISOString()}`);
}, 10 * 60 * 1000);

console.log('[SYSTEM] HydroTix Bridge — Render Edition siap!');
console.log('[SYSTEM] Menunggu data dari ESP32...\n');

// ── DUMMY WEB SERVER UNTUK RENDER.COM ──────────────────────────────
const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
  res.send('HydroTix MQTT Bridge is Running 24/7!');
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[SERVER] Dummy web server berjalan di port ${PORT}`);
});