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
});

client.on('reconnect', () => console.log('[BRIDGE] Reconnecting...'));
client.on('error',     (err) => console.error('[BRIDGE] Error:', err.message));
client.on('offline',   () => console.log('[BRIDGE] Offline'));

// ── MESSAGE HANDLER ────────────────────────────────────────────────
client.on('message', async (topic, message) => {
  const parts = topic.split('/');
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
  if (type === 'heartbeat') await handleHeartbeat(userId, deviceId);
});

// ── HANDLER TELEMETRY ──────────────────────────────────────────────
async function handleTelemetry(userId, deviceId, data) {
  console.log(`\n[TELEMETRY] device: ${deviceId} | user: ${userId}`);
  const now = admin.firestore.FieldValue.serverTimestamp();

  try {
    // 1. Simpan ke devices/{deviceId}/readings (sesuai schema Reading)
    const ref = await db
      .collection('devices')
      .doc(deviceId)
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

    // 2. Update dokumen utama device (merge agar field lain tetap ada)
    await db.collection('devices').doc(deviceId).set({
      id:       deviceId,
      ownerId:  userId,
      status:   'online',
      isOnline: true,
      lastSeen: now,
      // Cache nilai terakhir di dokumen utama (bisa dipakai mobile app)
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
        userId:    userId,
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
      userId:    userId,
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
async function handleHeartbeat(userId, deviceId) {
  console.log(`[HEARTBEAT] 💓 ${deviceId} masih online`);
  try {
    await db.collection('devices').doc(deviceId).set({
      id:       deviceId,
      ownerId:  userId,
      status:   'online',
      isOnline: true,
      lastSeen: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
  } catch (err) {
    console.error(`[ERROR] Heartbeat ${deviceId}:`, err.message);
  }
}

// ── DETEKSI DEVICE OFFLINE ─────────────────────────────────────────
// Tiap 5 menit — device yang lastSeen > 5 menit → set offline
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
