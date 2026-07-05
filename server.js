const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');

const MIME = {
  '.html': 'text/html',
  '.css':  'text/css',
  '.js':   'application/javascript',
};

// ── HTTP ──────────────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const urlPath = req.url.split('?')[0];
  let filePath = path.join(PUBLIC, urlPath === '/' ? 'index.html' : urlPath);
  if (!filePath.startsWith(PUBLIC)) { res.writeHead(403); res.end('Forbidden'); return; }
  const ext = path.extname(filePath);
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'text/plain' });
    res.end(data);
  });
});

// ── ROOMS ─────────────────────────────────────────────────────────────────────
// rooms: { code -> { hostId, players:[{id,name,ready,role,usedVote}], settings, started, vote, timer } }
// vote: { initiatorId, targetId, targetName, votes:{id->bool}, resolved }
const rooms = {};
const spyNotes = {}; // roomCode -> playerId -> Set of crossed heroes
const clients = {}; // ws -> { roomCode, playerId }

function makeCode() {
  return Math.random().toString(36).slice(2,6).toUpperCase();
}
function makePlayerId(room) {
  const used = new Set(room.players.map(p => p.id));
  let n = 1;
  while (used.has('p' + n)) n++;
  return 'p' + n;
}
function broadcast(roomCode, msg) {
  const room = rooms[roomCode];
  if (!room) return;
  const data = JSON.stringify(msg);
  room.players.forEach(p => {
    if (p.ws && p.ws.readyState === 1) p.ws.send(data);
  });
}

// ── SCORES ──
function standings(room) {
  return room.players
    .map(p => ({ id: p.id, name: p.name, score: p.score || 0 }))
    .sort((a, b) => b.score - a.score);
}
function addScore(room, id, pts) {
  const p = room.players.find(x => x.id === id);
  if (p) p.score = (p.score || 0) + pts;
}
// Шпиона выгнали голосованием: инициатор +2, остальные мирные +1, шпионы 0
function awardKickSpy(room, initiatorId) {
  const spyIds = new Set(room.players.filter(p => p.role === 'spy').map(p => p.id));
  addScore(room, initiatorId, 2);
  room.players.forEach(p => {
    if (p.id !== initiatorId && !spyIds.has(p.id)) addScore(room, p.id, 1);
  });
}
function finishRound(roomCode, resultMsg) {
  const room = rooms[roomCode];
  if (!room) return;
  room.finished = true;
  room.pendingRedemption = null;
  broadcast(roomCode, { ...resultMsg, scores: standings(room) });
}

// ── ПОДСКАЗКИ ШПИОНУ ПО ТАЙМЕРУ (elimination) ──
// План подсказок в % от общего времени раунда. Шпион может назвать героя в любой
// момент -> расписание сдвинуто позже. Роль/позицию не даём — только тип атаки, затем атрибут.
function buildHintPlan(room, poolHeroes) {
  const minutes = room.settings.time;
  const totalSec = room.settings.time * 60;
  let n;
  if (minutes <= 3) n = 0;
  else if (minutes <= 7) n = 1;
  else n = 2; // 8-14 и 15+ (потолок 2)
  if (poolHeroes.length < 30) n = Math.max(0, n - 1); // маленький пул -> меньше подсказок
  const SPY_CAN_GUESS_ANYTIME = true;
  const pcts = SPY_CAN_GUESS_ANYTIME
    ? (n === 1 ? [0.70] : n === 2 ? [0.65, 0.88] : [])
    : (n === 1 ? [0.60] : n === 2 ? [0.55, 0.80] : []);
  const kinds = ['atk', 'attr']; // слабая -> сильная
  const attrMode = (room.settings.attrMode && room.settings.attrMode !== 'any') ? room.settings.attrMode : null;
  const plan = [];
  for (let i = 0; i < n; i++) {
    const kind = kinds[i];
    if (kind === 'attr' && attrMode) continue; // режим «по атрибуту» -> подсказка атрибута бесполезна
    plan.push({ thresholdSec: Math.round(pcts[i] * totalSec), kind, fired: false });
  }
  return plan;
}

// Сузить spyPool по типу атаки / атрибуту загаданного. Возвращает true, если что-то удалено.
function applyHint(room, kind) {
  const secret = room.secretHero;
  if (!secret || !Array.isArray(room.spyPool)) return false;
  const before = room.spyPool.length;
  const next = (kind === 'atk')
    ? room.spyPool.filter(h => h.atk === secret.atk)
    : room.spyPool.filter(h => (h.attr || 'all') === (secret.attr || 'all'));
  if (!next.some(h => h.name === secret.name)) return false; // sanity: загаданный не должен выпадать
  if (next.length >= before) return false;                   // ничего не удалено -> не выдаём
  room.spyPool = next;
  return true;
}

function sendSpyPool(room) {
  const names = (room.spyPool || []).map(h => h.name);
  room.players.filter(p => p.role === 'spy').forEach(p => {
    if (p.ws && p.ws.readyState === 1) p.ws.send(JSON.stringify({ type: 'spy_pool', pool: names }));
  });
}

function processHints(room, roomCode) {
  if (!room.started || room.finished || !Array.isArray(room.hintPlan)) return;
  const totalSec = room.settings.time * 60;
  const remaining = room.timer ? room.timer.remaining : totalSec;
  const elapsed = totalSec - remaining;
  for (const h of room.hintPlan) {
    if (h.fired || elapsed < h.thresholdSec) continue;
    h.fired = true; // порог пройден — оцениваем один раз
    if (applyHint(room, h.kind)) {
      sendSpyPool(room);
      broadcast(roomCode, { type: 'hint_notice' }); // нейтрально, всем, без деталей
    }
  }
}

// Таймер истёк -> побеждает шпион. Сразу +3; догадка верна -> ещё +2 (итого 5).
function handleTimeout(room, roomCode) {
  if (!room.started || room.finished || room.timedOut) return;
  const remaining = room.timer ? room.timer.remaining : 1;
  if (remaining > 0) return;
  room.timedOut = true;
  const spyIds = room.players.filter(p => p.role === 'spy').map(p => p.id);
  spyIds.forEach(id => addScore(room, id, 3));
  room.pendingTimeout = { spyIds };
  broadcast(roomCode, { type: 'time_up', spyIds, scores: standings(room) });
  clearTimeout(room._timeoutFallback);
  room._timeoutFallback = setTimeout(() => {
    const r = rooms[roomCode];
    if (!r || !r.pendingTimeout) return;
    r.pendingTimeout = null;
    finishRound(roomCode, {
      type: 'guess_result', timeout: true, noGuess: true, guess: null, correct: false,
      secretHero: r.secretHero || null, spyId: null, spyName: null, winner: 'spy', spyIds,
    });
  }, 30000);
}

// Spy notes functions
function getSpyNotes(roomCode, playerId) {
  if (!spyNotes[roomCode]) spyNotes[roomCode] = {};
  if (!spyNotes[roomCode][playerId]) spyNotes[roomCode][playerId] = new Set();
  return Array.from(spyNotes[roomCode][playerId]);
}

function updateSpyNotes(roomCode, playerId, crossedHeroes) {
  if (!spyNotes[roomCode]) spyNotes[roomCode] = {};
  spyNotes[roomCode][playerId] = new Set(crossedHeroes);
}

function clearSpyNotes(roomCode, playerId) {
  if (spyNotes[roomCode] && spyNotes[roomCode][playerId]) {
    spyNotes[roomCode][playerId] = new Set();
  }
}
function roomState(roomCode) {
  const room = rooms[roomCode];
  return {
    type: 'room_state',
    code: roomCode,
    hostId: room.hostId,
    players: room.players.map(p => ({
      id: p.id,
      name: p.name,
      ready: p.ready,
      score: p.score || 0,
      online: !!(p.ws && p.ws.readyState === 1),
    })),
    settings: room.settings,
    started: room.started,
    vote: room.vote,
    timer: room.timer,
    // Add timer sync info if game started
    gameInfo: room.started ? {
      roles: room.players.map(p => ({ id: p.id, role: p.role })),
      usedVotes: room.players.map(p => ({ id: p.id, usedVote: p.usedVote }))
    } : null
  };
}

function checkVoteResult(roomCode) {
  const room = rooms[roomCode];
  if (!room || !room.vote || room.vote.resolved) return;
  const total = room.players.length;
  const voted = Object.keys(room.vote.votes).length;
  const yesCount = Object.values(room.vote.votes).filter(Boolean).length;
  const noCount  = voted - yesCount;
  const needed   = Math.floor(total / 2) + 1; // majority

  const majority = yesCount >= needed || noCount >= needed || voted === total;
  if (!majority) return;

  room.vote.resolved = true;
  const kicked = yesCount >= needed;
  const target = room.players.find(p => p.id === room.vote.targetId);
  const initiatorId = room.vote.initiatorId;
  const spyIds = room.players.filter(p => p.role === 'spy').map(p => p.id);
  const base = {
    type: 'vote_result',
    targetId: room.vote.targetId,
    targetName: room.vote.targetName,
    targetRole: target ? target.role : null,
    yesCount,
    noCount,
    spyIds,
  };

  // Большинство против — никого не выгнали, очки не начисляем
  if (!kicked) {
    broadcast(roomCode, { ...base, kicked: false, winner: null, scores: standings(room) });
    return;
  }

  if (target && target.role === 'spy') {
    // Поймали шпиона
    if (room.settings.catchButNotLost) {
      // «Пойман, но не проиграл» — даём шпиону последний шанс угадать
      room.pendingRedemption = { spyId: target.id, initiatorId };
      broadcast(roomCode, {
        ...base, kicked: true, winner: null,
        redemption: true, redemptionSpyId: target.id,
        scores: standings(room),
      });
      return;
    }
    // Обычная поимка: инициатор +2, остальные мирные +1
    awardKickSpy(room, initiatorId);
    finishRound(roomCode, { ...base, kicked: true, winner: 'civilians' });
    return;
  }

  // Выгнали мирного — шпион(ы) побеждают, +5 каждому шпиону
  if (target) {
    spyIds.forEach(id => addScore(room, id, 5));
    finishRound(roomCode, { ...base, kicked: true, winner: 'spy' });
  }
}

const ROOM_EMPTY_TIMEOUT = 10 * 60 * 1000;
const RECONNECT_GRACE = 12000; // 12 сек — только для F5 / перезагрузки, слот сразу свободен

function activePlayers(room) {
  return room.players.filter(p => p.ws && p.ws.readyState === 1);
}

function cleanGrace(room) {
  if (!room.reconnectGrace) return;
  const now = Date.now();
  for (const [id, g] of Object.entries(room.reconnectGrace)) {
    if (g.expiresAt <= now) delete room.reconnectGrace[id];
  }
}

function transferHostIfNeeded(room) {
  if (!room.players.some(p => p.id === room.hostId)) {
    const online = activePlayers(room);
    room.hostId = (online[0] || room.players[0])?.id;
  }
  if (room.hostId) {
    const host = room.players.find(p => p.id === room.hostId);
    if (host) host.ready = true;
  }
}

function removePlayerImmediate(roomCode, playerId, { allowRejoin = false } = {}) {
  const room = rooms[roomCode];
  if (!room) return;
  const player = room.players.find(p => p.id === playerId);
  if (!player || player.ws) return;

  if (allowRejoin && !room.started) {
    if (!room.reconnectGrace) room.reconnectGrace = {};
    room.reconnectGrace[playerId] = {
      name: player.name,
      ready: player.ready,
      wasHost: room.hostId === playerId,
      score: player.score || 0,
      expiresAt: Date.now() + RECONNECT_GRACE,
    };
  }

  const wasHost = room.hostId === playerId;
  room.players = room.players.filter(p => p.id !== playerId);

  if (room.players.length === 0) {
    delete rooms[roomCode];
    delete spyNotes[roomCode];
    return;
  }

  if (wasHost) transferHostIfNeeded(room);
  broadcast(roomCode, roomState(roomCode));
}

function buildGameAssignment(room, player, roomCode) {
  const isSpy = player.role === 'spy';
  const hero = room.secretHero;
  return {
    type: 'game_start',
    role: player.role,
    hero: isSpy ? null : hero,
    location: isSpy ? null : hero,
    time: room.settings.time,
    players: room.players.map(x => ({ id: x.id, name: x.name })),
    // Рандомная очередь разговора — фиксируется один раз за игру
    speakingOrder: (room.speakingOrder || room.players.map(x => x.id))
      .map(id => room.players.find(x => x.id === id))
      .filter(Boolean)
      .map(x => ({ id: x.id, name: x.name })),
    myId: player.id,
    spyNotes: isSpy ? getSpyNotes(roomCode, player.id) : [],
    spyPool: isSpy ? (room.spyPool || []).map(h => h.name) : null,
    hostId: room.hostId,
    initialTimer: room.timer?.remaining || room.settings.time * 60,
    scores: standings(room),
    catchButNotLost: !!room.settings.catchButNotLost,
  };
}

function sendGameStart(room, player) {
  if (!player.gameAssignment) return;
  if (player.ws && player.ws.readyState === 1) {
    player.ws.send(JSON.stringify(player.gameAssignment));
  }
}

function removePlayer(roomCode, playerId) {
  removePlayerImmediate(roomCode, playerId, { allowRejoin: false });
}

function handleDisconnect(roomCode, playerId) {
  const room = rooms[roomCode];
  if (!room) return;

  if (activePlayers(room).length === 0) {
    clearTimeout(room._emptyTimer);
    room._emptyTimer = setTimeout(() => {
      if (rooms[roomCode] && activePlayers(rooms[roomCode]).length === 0) {
        delete rooms[roomCode];
        delete spyNotes[roomCode];
      }
    }, ROOM_EMPTY_TIMEOUT);
    return;
  }

  if (!room.started) {
    removePlayerImmediate(roomCode, playerId, { allowRejoin: true });
    return;
  }

  broadcast(roomCode, roomState(roomCode));
}

function removeGhostsByClient(room, clientId, exceptId) {
  if (!clientId) return;
  const ghosts = room.players.filter(p => p.clientId === clientId && p.id !== exceptId);
  ghosts.forEach(g => {
    room.players = room.players.filter(p => p.id !== g.id);
    if (room.reconnectGrace) delete room.reconnectGrace[g.id];
  });
}

const wss = new WebSocketServer({ server });

// Keepalive — мягкая проверка (не рвём соединение агрессивно)
const pingInterval = setInterval(() => {
  wss.clients.forEach(client => {
    if (client.isAlive === false) return client.terminate();
    client.isAlive = false;
    try { client.ping(); } catch {}
  });
}, 45000);
wss.on('close', () => clearInterval(pingInterval));

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let myRoom = null;
  let myId   = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── create room
    if (msg.type === 'create') {
      let code;
      do { code = makeCode(); } while (rooms[code]);
      myRoom = code;
      myId   = 'p1';
      rooms[code] = {
        hostId: myId,
        players: [{ id: myId, name: msg.name || 'Хост', ws, ready: true, clientId: msg.clientId, score: 0 }],
        settings: { multipleSpies: false, catchButNotLost: false, attrMode: 'any', time: 8, maxPlayers: 4 },
        started: false,
        reconnectGrace: {},
      };
      ws.send(JSON.stringify({ type: 'created', code, playerId: myId }));
      broadcast(code, roomState(code));
    }

    // ── join room
    else if (msg.type === 'join') {
      const code = (msg.code || '').toUpperCase();
      const room = rooms[code];
      if (!room) { ws.send(JSON.stringify({ type: 'error', text: 'Комната не найдена. Попросите хоста создать новую комнату и держать лобби открытым.' })); return; }
      if (room.started) { ws.send(JSON.stringify({ type: 'error', text: 'Игра уже началась' })); return; }
      cleanGrace(room);
      // Тот же браузер уже в комнате? Переиспользуем его слот (без дублей)
      const sameClient = msg.clientId && room.players.find(p => p.clientId === msg.clientId);
      if (sameClient) {
        sameClient.ws = ws;
        if (msg.name) sameClient.name = msg.name;
        myRoom = code;
        myId   = sameClient.id;
        ws.send(JSON.stringify({ type: 'joined', code, playerId: myId, roomState: roomState(code) }));
        broadcast(code, roomState(code));
        return;
      }
      const max = room.settings.maxPlayers || 12;
      if (room.players.length >= max) { ws.send(JSON.stringify({ type: 'error', text: `Комната заполнена (${max} игроков)` })); return; }
      myRoom = code;
      myId   = makePlayerId(room);
      room.players.push({ id: myId, name: msg.name || `Игрок ${room.players.length + 1}`, ws, ready: false, clientId: msg.clientId, score: 0 });
      ws.send(JSON.stringify({ type: 'joined', code, playerId: myId, roomState: roomState(code) }));
      broadcast(code, roomState(code));
    }

    // ── rejoin (page reload / navigation within the app)
    else if (msg.type === 'rejoin') {
      const code = (msg.code || '').toUpperCase();
      const room = rooms[code];
      if (!room) { ws.send(JSON.stringify({ type: 'error', text: 'Комната не найдена. Попросите хоста создать новую комнату и держать лобби открытым.' })); return; }
      cleanGrace(room);
      const existing = room.players.find(p => p.id === msg.playerId);
      if (existing) {
        existing.ws = ws;
        if (msg.name) existing.name = msg.name;
        if (msg.clientId) existing.clientId = msg.clientId;
        clearTimeout(room._emptyTimer);
        myRoom = code;
        myId   = existing.id;

        const state = roomState(code);
        ws.send(JSON.stringify({
          type: 'rejoined',
          code,
          playerId: myId,
          roomState: state,
          gameData: room.started ? (existing.gameAssignment || buildGameAssignment(room, existing, code)) : null,
        }));

        if (room.started) sendGameStart(room, existing);
        broadcast(code, state);
      } else {
        // Сессия/playerId потеряны, но это тот же браузер — переиспользуем слот
        const sameClient = msg.clientId && room.players.find(p => p.clientId === msg.clientId);
        if (sameClient) {
          sameClient.ws = ws;
          if (msg.name) sameClient.name = msg.name;
          clearTimeout(room._emptyTimer);
          myRoom = code;
          myId   = sameClient.id;
          const state = roomState(code);
          ws.send(JSON.stringify({
            type: 'rejoined', code, playerId: myId, roomState: state,
            gameData: room.started ? (sameClient.gameAssignment || buildGameAssignment(room, sameClient, code)) : null,
          }));
          if (room.started) sendGameStart(room, sameClient);
          broadcast(code, state);
          return;
        }
        const grace = room.reconnectGrace?.[msg.playerId];
        if (grace && Date.now() < grace.expiresAt) {
          delete room.reconnectGrace[msg.playerId];
          removeGhostsByClient(room, msg.clientId);
          myRoom = code;
          myId   = msg.playerId;
          room.players.push({
            id: myId,
            name: msg.name || grace.name,
            ws,
            ready: grace.ready,
            clientId: msg.clientId,
            score: grace.score || 0,
          });
          if (grace.wasHost) room.hostId = myId;
          clearTimeout(room._emptyTimer);

          const state = roomState(code);
          ws.send(JSON.stringify({ type: 'rejoined', code, playerId: myId, roomState: state, gameData: null }));
          broadcast(code, state);
        } else {
          if (grace) delete room.reconnectGrace[msg.playerId];
          if (room.started) { ws.send(JSON.stringify({ type: 'error', text: 'Игра уже началась' })); return; }
          removeGhostsByClient(room, msg.clientId);
          const max = room.settings.maxPlayers || 12;
          if (room.players.length >= max) { ws.send(JSON.stringify({ type: 'error', text: `Комната заполнена (${max} игроков)` })); return; }
          myRoom = code;
          myId   = makePlayerId(room);
          room.players.push({ id: myId, name: msg.name || `Игрок ${room.players.length + 1}`, ws, ready: false, clientId: msg.clientId, score: 0 });
          ws.send(JSON.stringify({ type: 'joined', code, playerId: myId, roomState: roomState(code) }));
          broadcast(code, roomState(code));
        }
      }
    }

    // ── sync room state (fallback if broadcast missed)
    else if (msg.type === 'sync') {
      const room = rooms[myRoom];
      if (room) ws.send(JSON.stringify(roomState(myRoom)));
    }

    // ── leave room (выход без закрытия сайта)
    else if (msg.type === 'leave') {
      const room = rooms[myRoom];
      if (room) {
        const p = room.players.find(x => x.id === myId);
        if (p) p.ws = null;
        if (room.reconnectGrace) delete room.reconnectGrace[myId];
        removePlayerImmediate(myRoom, myId, { allowRejoin: false });
      }
      try { ws.send(JSON.stringify({ type: 'left' })); } catch {}
      myRoom = null;
      myId   = null;
    }

    // ── update settings (host only)
    else if (msg.type === 'settings') {
      const room = rooms[myRoom];
      if (!room || room.hostId !== myId || room.started) return;
      const next = msg.settings || {};
      const clamp = (value, min, max, fallback) => {
        const n = Number.parseInt(value, 10);
        if (!Number.isFinite(n)) return fallback;
        return Math.max(min, Math.min(max, n));
      };
      room.settings = {
        ...room.settings,
        maxPlayers: Math.max(room.players.length, clamp(next.maxPlayers, 3, 12, room.settings.maxPlayers || 4)),
        multipleSpies: !!next.multipleSpies,
        catchButNotLost: !!next.catchButNotLost,
        attrMode: ['any','str','agi','int','all'].includes(next.attrMode) ? next.attrMode : (room.settings.attrMode || 'any'),
        time: clamp(next.time, 4, 20, room.settings.time || 8),
      };
      broadcast(myRoom, roomState(myRoom));
    }

    else if (msg.type === 'kick_player') {
      const room = rooms[myRoom];
      if (!room || room.hostId !== myId || room.started) return;
      const target = room.players.find(p => p.id === msg.playerId);
      if (!target || target.id === myId) return;
      try { if (target.ws && target.ws.readyState === 1) target.ws.send(JSON.stringify({ type: 'kicked' })); } catch {}
      try { if (target.ws && target.ws.readyState === 1) target.ws.close(1000, 'kicked'); } catch {}
      room.players = room.players.filter(p => p.id !== target.id);
      if (room.reconnectGrace) delete room.reconnectGrace[target.id];
      if (spyNotes[myRoom]) delete spyNotes[myRoom][target.id];
      broadcast(myRoom, roomState(myRoom));
    }

    else if (msg.type === 'transfer_host') {
      const room = rooms[myRoom];
      if (!room || room.hostId !== myId || room.started) return;
      const target = room.players.find(p => p.id === msg.playerId);
      if (!target) return;
      room.hostId = target.id;
      target.ready = true;
      broadcast(myRoom, roomState(myRoom));
    }

    else if (msg.type === 'ready') {
      const room = rooms[myRoom];
      if (!room) return;
      const p = room.players.find(x => x.id === myId);
      if (p) p.ready = !p.ready;
      broadcast(myRoom, roomState(myRoom));
    }

    // ── start game (host only)
    else if (msg.type === 'start') {
      const room = rooms[myRoom];
      if (!room || room.hostId !== myId) return;

      // Check minimum players
      if (room.players.length < 3) {
        ws.send(JSON.stringify({ type: 'error', text: 'Нужно минимум 3 игрока для начала игры!' }));
        return;
      }
      // pick a random hero (draft-ban + режим «по атрибуту»)
      const { heroes = [], disabledHeroes = [] } = msg;
      let available = heroes.filter(h => !disabledHeroes.includes(h.name));
      const attrMode = (room.settings.attrMode && room.settings.attrMode !== 'any') ? room.settings.attrMode : null;
      if (attrMode) available = available.filter(h => (h.attr || 'all') === attrMode);
      if (available.length === 0) {
        ws.send(JSON.stringify({ type: 'error', text: 'Нет доступных персонажей для выбранного режима!' }));
        return;
      }
      const hero = available[Math.floor(Math.random() * available.length)];
      room.secretHero = hero;
      // Пул для сетки шпиона (сужается подсказками) + план подсказок
      room.spyPool = available.slice();
      room.hintPlan = buildHintPlan(room, available);
      room.timedOut = false;
      room.pendingTimeout = null;
      clearTimeout(room._timeoutFallback);
      if (spyNotes[myRoom]) spyNotes[myRoom] = {}; // сброс вычёркиваний прошлой игры

      const n = room.players.length;
      const maxSpies = Math.max(1, n - 1);
      const spyCount = room.settings.multipleSpies ? (1 + Math.floor(Math.random() * maxSpies)) : 1;
      room.actualSpyCount = spyCount;

      const indices = [...Array(n).keys()].sort(() => Math.random() - .5);
      const spyIds = new Set(indices.slice(0, spyCount).map(i => room.players[i].id));

      room.started = true;
      room.finished = false;
      room.pendingRedemption = null;
      room.timer = { remaining: room.settings.time * 60, updatedAt: Date.now() };
      // Рандомная очередь разговора на эту игру
      room.speakingOrder = room.players.map(p => p.id).sort(() => Math.random() - .5);
      room.players.forEach(p => {
        p.role = spyIds.has(p.id) ? 'spy' : 'civilian';
        p.usedVote = false;
        p.gameAssignment = buildGameAssignment(room, p, myRoom);
        sendGameStart(room, p);
      });
      room.vote = null;
    }

    // ── timer sync (host broadcasts to all)
    else if (msg.type === 'timer') {
      const room = rooms[myRoom];
      if (room) {
        room.timer = { remaining: msg.remaining, updatedAt: Date.now() };
        processHints(room, myRoom);
        handleTimeout(room, myRoom);
      }
      broadcast(myRoom, { type: 'timer', remaining: msg.remaining });
    }

    // ── start vote
    else if (msg.type === 'vote_start') {
      const room = rooms[myRoom];
      if (!room || !room.started || room.finished) return;
      const initiator = room.players.find(p => p.id === myId);
      if (!initiator || initiator.usedVote) {
        ws.send(JSON.stringify({ type: 'error', text: 'Ты уже запускал голосование!' }));
        return;
      }
      if (room.vote && !room.vote.resolved) {
        ws.send(JSON.stringify({ type: 'error', text: 'Голосование уже идёт!' }));
        return;
      }
      const target = room.players.find(p => p.id === msg.targetId);
      if (!target) return;
      initiator.usedVote = true;
      room.vote = { initiatorId: myId, targetId: msg.targetId, targetName: target.name, votes: {}, resolved: false };
      // initiator auto-votes yes
      room.vote.votes[myId] = true;
      broadcast(myRoom, {
        type: 'vote_started',
        initiatorId: myId,
        initiatorName: initiator.name,
        targetId: msg.targetId,
        targetName: target.name,
        votes: room.vote.votes,
        total: room.players.length,
      });
      checkVoteResult(myRoom);
    }

    // ── cast vote
    else if (msg.type === 'vote_cast') {
      const room = rooms[myRoom];
      if (!room || !room.vote || room.vote.resolved) return;
      room.vote.votes[myId] = msg.yes;
      broadcast(myRoom, {
        type: 'vote_update',
        votes: room.vote.votes,
        total: room.players.length,
      });
      checkVoteResult(myRoom);
    }

    // ── spy notes update
    else if (msg.type === 'spy_notes_update') {
      const room = rooms[myRoom];
      if (!room || !room.started) return;
      const player = room.players.find(p => p.id === myId);
      if (!player || player.role !== 'spy') return;
      updateSpyNotes(myRoom, myId, msg.crossedHeroes || []);
      ws.send(JSON.stringify({ type: 'spy_notes_saved' }));
    }

    // ── spy notes clear
    else if (msg.type === 'spy_notes_clear') {
      const room = rooms[myRoom];
      if (!room || !room.started) return;
      const player = room.players.find(p => p.id === myId);
      if (!player || player.role !== 'spy') return;
      clearSpyNotes(myRoom, myId);
      ws.send(JSON.stringify({ type: 'spy_notes_cleared' }));
    }

    // ── spy guess (шпион угадывает загаданного героя, в т.ч. последний шанс)
    else if (msg.type === 'spy_guess') {
      const room = rooms[myRoom];
      if (!room || !room.started || room.finished) return;
      const player = room.players.find(p => p.id === myId);
      if (!player || player.role !== 'spy') {
        ws.send(JSON.stringify({ type: 'error', text: 'Угадывать может только шпион!' }));
        return;
      }
      const secret = room.secretHero && room.secretHero.name;
      const guess = msg.heroName;
      const correct = !!secret && guess === secret;
      const spyIds = room.players.filter(p => p.role === 'spy').map(p => p.id);

      // Таймаут: шпион уже победил (+3), догадка — только за бонус (+2 -> итого 5)
      if (room.pendingTimeout) {
        clearTimeout(room._timeoutFallback);
        if (correct) spyIds.forEach(id => addScore(room, id, 2));
        room.pendingTimeout = null;
        finishRound(myRoom, {
          type: 'guess_result', timeout: true, guess, correct,
          secretHero: room.secretHero || null, spyId: myId, spyName: player.name,
          winner: 'spy', spyIds,
        });
        return;
      }

      const redemption = !!(room.pendingRedemption && room.pendingRedemption.spyId === myId);
      if (!redemption && room.vote && !room.vote.resolved) {
        ws.send(JSON.stringify({ type: 'error', text: 'Дождись окончания голосования!' }));
        return;
      }
      if (redemption) {
        if (correct) spyIds.forEach(id => addScore(room, id, 5));
        else awardKickSpy(room, room.pendingRedemption.initiatorId);
      } else {
        if (correct) spyIds.forEach(id => addScore(room, id, 5));
        else room.players.forEach(p => { if (!spyIds.includes(p.id)) addScore(room, p.id, 1); });
      }
      finishRound(myRoom, {
        type: 'guess_result', guess, correct,
        secretHero: room.secretHero || null, spyId: myId, spyName: player.name,
        winner: correct ? 'spy' : 'civilians', spyIds, redemption,
      });
    }

    // ── restart
    else if (msg.type === 'restart') {
      const room = rooms[myRoom];
      if (!room || room.hostId !== myId) return;
      room.started = false;
      room.finished = false;
      room.secretHero = null;
      room.speakingOrder = null;
      room.vote = null;
      room.pendingRedemption = null;
      room.spyPool = null;
      room.hintPlan = null;
      room.timedOut = false;
      room.pendingTimeout = null;
      clearTimeout(room._timeoutFallback);
      if (spyNotes[myRoom]) spyNotes[myRoom] = {};
      room.players.forEach(p => {
        p.ready = p.id === room.hostId;
        p.role = null;
        p.usedVote = false;
        p.gameAssignment = null;
      });
      broadcast(myRoom, roomState(myRoom));
    }
  });

  ws.on('close', () => {
    if (!myRoom || !rooms[myRoom]) return;
    const room = rooms[myRoom];
    const player = room.players.find(p => p.id === myId);
    // Игнорируем закрытие устаревшего сокета (перезагрузка / переподключение)
    if (!player || player.ws !== ws) return;
    player.ws = null;
    handleDisconnect(myRoom, myId);
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`Дота-Шпион запущен на порту ${PORT}`));
