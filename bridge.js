// hydrotix-bridge/bridge.js
require('dotenv').config();
const mqtt  = require('mqtt');
const admin = require('firebase-admin');

// ── Firebase Admin ──────────
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

  client.subscribe('hydro/+/+/telemetry', { qos: 1 });
  client.subscribe('hydro/+/+/heartbeat', { qos: 1 });
  client.subscribe('hydro/setup/+/request', { qos: 1 });
});

client.on('reconnect', () => console.log('[BRIDGE] Reconnecting...'));
client.on('error',     (err) => console.error('[BRIDGE] Error:', err.message));
client.on('offline',   () => console.log('[BRIDGE] Offline'));

// ── MESSAGE HANDLER ────────────────────────────────────────────────
client.on('message', async (topic, message) => {
  const parts = topic.split('/');
  
  if (parts.length === 4 && parts[0] === 'hydro' && parts[1] === 'setup' && parts[3] === 'request') {
    await handleZtpRequest(parts[2]);
    return;
  }

  if (parts.length !== 4 || parts[0] !== 'hydro') return;

  const [, userId, deviceId, type] = parts;
  let data;
  try {
    data = JSON.parse(message.toString());
  } catch (e) {
    return;
  }

  if (type === 'telemetry') await handleTelemetry(userId, deviceId, data);
  if (type === 'heartbeat') await handleHeartbeat(userId, deviceId, data); 
});

// ── HANDLER ZTP ──────────────────────────
async function handleZtpRequest(macAddress) {
  try {
    const devicesRef = db.collection('devices');
    const snapshot = await devicesRef.where('macAddress', '==', macAddress).limit(1).get();

    if (snapshot.empty) return;
    const doc = snapshot.docs[0];
    const data = doc.data();

    if (!data.ownerId) return;

    const payload = JSON.stringify({ ownerId: data.ownerId, deviceId: doc.id });
    client.publish(`hydro/setup/${macAddress}/response`, payload, { qos: 1 });
    console.log(`[ZTP] ✅ Berhasil mengirim profil ke MAC: ${macAddress}`);
  } catch (err) {
    console.error(`[ERROR] ZTP:`, err.message);
  }
}

// ── HANDLER TELEMETRY ──────────────────────────────────────────────
async function handleTelemetry(userId, deviceId, data) {
  const now = admin.firestore.FieldValue.serverTimestamp();
  try {
    const deviceRef = db.collection('devices').doc(deviceId);
    const deviceSnap = await deviceRef.get();

    if (!deviceSnap.exists) {
      client.publish(`hydro/${userId}/${deviceId}/command`, JSON.stringify({ target: "factory_reset", state: true }), { qos: 1 });
      return;
    }

    const validOwnerId = deviceSnap.data().ownerId;

    await deviceRef.collection('readings').add({
      timestamp: now,
      ph: data.ph ?? null, tds: data.tds ?? null,
      waterTemp: data.waterTemp ?? null, airTemp: data.airTemp ?? null,
      humidity: data.humidity ?? null, waterLevel: data.waterLevel ?? null,
      do: data.do ?? null, alert: data.alert ?? false,
      relayStates: data.relayStates ?? {},
    });

    // Sinkronisasi lastReading DAN state relay aktual dari ESP32 ke Firestore.
    // Dengan ini, field 'relays' di Firestore selalu mencerminkan state nyata hardware,
    // BUKAN hanya perintah terakhir dari HP. Ini sumber kebenaran tunggal.
    const updatePayload = {
      status: 'online', isOnline: true, lastSeen: now,
      lastReading: {
        ph: data.ph ?? null, tds: data.tds ?? null,
        waterTemp: data.waterTemp ?? null, airTemp: data.airTemp ?? null,
        humidity: data.humidity ?? null, waterLevel: data.waterLevel ?? null,
        do: data.do ?? null, alert: data.alert ?? false, updatedAt: now,
      },
    };

    // Sinkronisasi state relay dari telemetry (relayStates dikirim ESP32)
    if (data.relayStates && typeof data.relayStates === 'object') {
      updatePayload.relays = data.relayStates;
    }

    await deviceRef.set(updatePayload, { merge: true });

    if (data.alert === true) {
      await db.collection('alerts').add({
        deviceId: deviceId, userId: validOwnerId,
        parameter: 'sensor_anomaly', severity: 'warning',
        value: data.waterTemp ?? 0, message: `Anomali sensor terdeteksi pada alat`,
        status: 'unread', createdAt: now,
      });
    }
  } catch (err) {
    console.error(`[ERROR] Telemetry:`, err.message);
  }
}


// ── HANDLER HEARTBEAT ──────────────────────────────────────────────
async function handleHeartbeat(userId, deviceId, data = {}) {
  try {
    const deviceRef = db.collection('devices').doc(deviceId);
    const deviceSnap = await deviceRef.get();

    if (!deviceSnap.exists) {
      client.publish(`hydro/${userId}/${deviceId}/command`, JSON.stringify({ target: "factory_reset", state: true }), { qos: 1 });
      return;
    }

    const update = { status: 'online', isOnline: true, lastSeen: admin.firestore.FieldValue.serverTimestamp() };
    if (data.macAddress) update.macAddress = data.macAddress;
    if (data.ip) update.ip = data.ip;
    if (data.firmware) update.firmwareVersion = data.firmware;

    await deviceRef.set(update, { merge: true });
  } catch (err) {}
}

// ── MESIN PENJADWAL OTOMATIS (SCHEDULER ENGINE) ────────────────────
function getWitaTime() {
  const d = new Date();
  // Konversi waktu ke zona WITA (Asia/Makassar) untuk lokasi Bali
  const options = { timeZone: 'Asia/Makassar', hour12: false, hour: '2-digit', minute: '2-digit' };
  const timeString = d.toLocaleTimeString('en-US', options); 
  
  const dBali = new Date(d.toLocaleString("en-US", {timeZone: "Asia/Makassar"}));
  let day = dBali.getDay(); // 0 = Minggu, 1 = Senin
  day = day === 0 ? 7 : day; // Konversi ke format 1 (Senin) - 7 (Minggu)
  
  return { timeString, day };
}

async function executeAutoCommand(ownerId, deviceId, actuatorId, state, actuatorName) {
  // 1. Tembak MQTT ke ESP32
  const topic = `hydro/${ownerId}/${deviceId}/command`;
  const payload = JSON.stringify({ target: actuatorId, state: state });
  client.publish(topic, payload, { qos: 1 });

  // 2. Update status relay di Database agar UI Web & Mobile sinkron
  await db.collection('devices').doc(deviceId).set({
    relays: {
      [actuatorId]: state
    }
  }, { merge: true });

  // 3. Catat di Logbook
  await db.collection('logs').add({
    type: 'scheduler',
    deviceId: deviceId,
    userId: ownerId,
    note: `[Otomatis] Penjadwalan menjalankan ${actuatorName}: ${state ? 'ON' : 'OFF'}`,
    currentPh: 0, currentTds: 0, currentTemp: 0, // Placeholder
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  });
  
  console.log(`[SCHEDULER] ⚡ Dieksekusi: ${deviceId} -> ${actuatorId} (${state ? 'ON' : 'OFF'})`);
}

async function checkAndExecuteSchedules() {
  const { timeString, day } = getWitaTime();
  
  try {
    const devicesSnap = await db.collection('devices').get();
    for (const doc of devicesSnap.docs) {
      const deviceId = doc.id;
      const ownerId = doc.data().ownerId;
      if (!ownerId) continue; // Lewati alat yang belum diklaim

      // Ambil jadwal aktif dari masing-masing alat
      const schedulesSnap = await db.collection('devices')
        .doc(deviceId)
        .collection('schedules')
        .where('isActive', '==', true)
        .get();

      for (const schedDoc of schedulesSnap.docs) {
        const sched = schedDoc.data();
        const activeDays = sched.activeDays || [];
        
        // Lewati jika hari ini tidak ada dalam daftar hari aktif jadwal
        if (!activeDays.includes(day)) continue;

        // Cocokkan jam dan menit saat ini (WITA) dengan startTime dan endTime
        if (sched.startTime === timeString) {
          await executeAutoCommand(ownerId, deviceId, sched.actuatorId, true, sched.actuatorName);
        } else if (sched.endTime === timeString) {
          await executeAutoCommand(ownerId, deviceId, sched.actuatorId, false, sched.actuatorName);
        }
      }
    }
  } catch (err) {
    console.error('[SCHEDULER] Error:', err.message);
  }
}

// ── KONTROL WAKTU (Cron Replacement) ───────────────────────────────
// Deteksi offline setiap 5 menit
setInterval(async () => {
  const cutoff = new Date(Date.now() - 5 * 60 * 1000);
  const snap = await db.collection('devices').where('isOnline', '==', true).where('lastSeen', '<', cutoff).get();
  if (snap.empty) return;
  const batch = db.batch();
  snap.forEach(doc => batch.update(doc.ref, { isOnline: false, status: 'offline' }));
  await batch.commit();
}, 5 * 60 * 1000);

// Pengecekan Jadwal dijalankan SETIAP 1 MENIT
setInterval(() => {
  checkAndExecuteSchedules();
}, 60 * 1000);

// Keep-Alive Ping untuk Log Render
setInterval(() => console.log(`[PING] 🟢 Bridge aktif — ${new Date().toISOString()}`), 10 * 60 * 1000);

console.log('[SYSTEM] HydroTix Bridge (With Scheduler) siap!');

// ── DUMMY WEB SERVER UNTUK RENDER.COM ──────────────────────────────
const express = require('express');
const app = express();
const PORT = process.env.PORT || 3000;
app.get('/', (req, res) => res.send('HydroTix MQTT Bridge is Running 24/7!'));
app.listen(PORT, '0.0.0.0', () => console.log(`[SERVER] Port ${PORT}`));