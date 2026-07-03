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
function broadcast(roomCode, msg) {
  const room = rooms[roomCode];
  if (!room) return;
  const data = JSON.stringify(msg);
  room.players.forEach(p => {
    if (p.ws && p.ws.readyState === 1) p.ws.send(data);
  });
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
  let winner = null;

  if (kicked) {
    const target = room.players.find(p => p.id === room.vote.targetId);
    if (target) {
      winner = target.role === 'spy' ? 'civilians' : 'spy'; // kicked spy → civilians win; kicked innocent → spy wins
    }
  }

  broadcast(roomCode, {
    type: 'vote_result',
    kicked,
    targetId: room.vote.targetId,
    targetName: room.vote.targetName,
    targetRole: room.players.find(p => p.id === room.vote.targetId)?.role,
    winner, // 'civilians' | 'spy' | null (no kick)
    yesCount,
    noCount,
    spyIds: room.players.filter(p => p.role === 'spy').map(p => p.id),
  });
}

const ROOM_EMPTY_TIMEOUT = 10 * 60 * 1000; // 10 мин — пустая комната
const PLAYER_DISCONNECT_GRACE = 30000; // 30 сек на переподключение (перезагрузка страницы)

function activePlayers(room) {
  return room.players.filter(p => p.ws && p.ws.readyState === 1);
}

function cancelDisconnectTimer(player) {
  if (player && player._disconnectTimer) {
    clearTimeout(player._disconnectTimer);
    player._disconnectTimer = null;
  }
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
    myId: player.id,
    spyNotes: isSpy ? getSpyNotes(roomCode, player.id) : [],
    hostId: room.hostId,
    initialTimer: room.timer?.remaining || room.settings.time * 60,
  };
}

function sendGameStart(room, player) {
  if (!player.gameAssignment) return;
  if (player.ws && player.ws.readyState === 1) {
    player.ws.send(JSON.stringify(player.gameAssignment));
  }
}

function removePlayer(roomCode, playerId) {
  const room = rooms[roomCode];
  if (!room) return;
  const player = room.players.find(p => p.id === playerId);
  if (!player || player.ws) return;

  room.players = room.players.filter(p => p.id !== playerId);
  cancelDisconnectTimer(player);

  if (room.players.length === 0) {
    delete rooms[roomCode];
    delete spyNotes[roomCode];
    return;
  }

  if (room.hostId === playerId) {
    const online = activePlayers(room);
    room.hostId = (online[0] || room.players[0]).id;
    if (room.hostId) {
      const newHost = room.players.find(p => p.id === room.hostId);
      if (newHost) newHost.ready = true;
    }
  }

  broadcast(roomCode, roomState(roomCode));
}

function schedulePlayerDisconnect(roomCode, playerId) {
  const room = rooms[roomCode];
  if (!room) return;
  const player = room.players.find(p => p.id === playerId);
  if (!player) return;

  cancelDisconnectTimer(player);
  player._disconnectTimer = setTimeout(() => {
    removePlayer(roomCode, playerId);
  }, PLAYER_DISCONNECT_GRACE);
}

function handleDisconnect(roomCode, playerId) {
  const room = rooms[roomCode];
  if (!room) return;

  const player = room.players.find(p => p.id === playerId);
  if (player) player.ws = null;

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
    schedulePlayerDisconnect(roomCode, playerId);
  }

  broadcast(roomCode, roomState(roomCode));
}

const wss = new WebSocketServer({ server });

// Keepalive — не даём Render разрывать «тихие» WebSocket-соединения
const pingInterval = setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);
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
        players: [{ id: myId, name: msg.name || 'Хост', ws, ready: true }],
        settings: { spies: 1, time: 8, maxPlayers: 4 },
        started: false,
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
      const max = room.settings.maxPlayers || 12;
      if (room.players.length >= max) { ws.send(JSON.stringify({ type: 'error', text: `Комната заполнена (${max} игроков)` })); return; }
      myRoom = code;
      myId   = 'p' + (room.players.length + 1);
      room.players.push({ id: myId, name: msg.name || `Игрок ${room.players.length + 1}`, ws, ready: false });
      ws.send(JSON.stringify({ type: 'joined', code, playerId: myId }));
      broadcast(code, roomState(code));
    }

    // ── rejoin (page reload / navigation within the app)
    else if (msg.type === 'rejoin') {
      const code = (msg.code || '').toUpperCase();
      const room = rooms[code];
      if (!room) { ws.send(JSON.stringify({ type: 'error', text: 'Комната не найдена. Попросите хоста создать новую комнату и держать лобби открытым.' })); return; }
      const existing = room.players.find(p => p.id === msg.playerId);
      if (existing) {
        existing.ws = ws;
        cancelDisconnectTimer(existing);
        clearTimeout(room._emptyTimer);
        myRoom = code;
        myId   = existing.id;

        const response = {
          type: 'rejoined',
          code,
          playerId: myId,
          roomState: roomState(code),
          gameData: room.started ? (existing.gameAssignment || buildGameAssignment(room, existing, code)) : null,
        };
        ws.send(JSON.stringify(response));

        if (room.started) sendGameStart(room, existing);
        else broadcast(code, roomState(code));
      } else {
        // player not found — treat as fresh join
        if (room.started) { ws.send(JSON.stringify({ type: 'error', text: 'Игра уже началась' })); return; }
        const max = room.settings.maxPlayers || 12;
        if (room.players.length >= max) { ws.send(JSON.stringify({ type: 'error', text: `Комната заполнена (${max} игроков)` })); return; }
        myRoom = code;
        myId   = 'p' + (room.players.length + 1);
        room.players.push({ id: myId, name: msg.name || `Игрок ${room.players.length + 1}`, ws, ready: false });
        ws.send(JSON.stringify({ type: 'joined', code, playerId: myId }));
        broadcast(code, roomState(code));
      }
    }

    // ── update settings (host only)
    else if (msg.type === 'settings') {
      const room = rooms[myRoom];
      if (!room || room.hostId !== myId) return;
      room.settings = { ...room.settings, ...msg.settings };
      broadcast(myRoom, roomState(myRoom));
    }

    // ── ready toggle
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
      
      // Check maximum spies
      const maxSpies = Math.min(room.players.length - 1, 3);
      if (room.settings.spies > maxSpies) {
        room.settings.spies = maxSpies;
        broadcast(myRoom, roomState(myRoom));
        ws.send(JSON.stringify({ type: 'error', text: `Количество шпионов уменьшено до ${maxSpies} (максимум для ${room.players.length} игроков)` }));
        return;
      }
      
      // pick a random hero from disabled list sent by host
      const { heroes = [], disabledHeroes = [] } = msg;
      const available = heroes.filter(h => !disabledHeroes.includes(h.name));
      if (available.length === 0) {
        ws.send(JSON.stringify({ type: 'error', text: 'Нет доступных персонажей!' }));
        return;
      }
      const hero = available[Math.floor(Math.random() * available.length)];
      room.secretHero = hero;

      const n = room.players.length;
      const spyCount = Math.min(room.settings.spies, n - 1);

      const indices = [...Array(n).keys()].sort(() => Math.random() - .5);
      const spyIds = new Set(indices.slice(0, spyCount).map(i => room.players[i].id));

      room.started = true;
      room.timer = { remaining: room.settings.time * 60, updatedAt: Date.now() };
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
      }
      broadcast(myRoom, { type: 'timer', remaining: msg.remaining });
    }

    // ── start vote
    else if (msg.type === 'vote_start') {
      const room = rooms[myRoom];
      if (!room || !room.started) return;
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

    // ── restart
    else if (msg.type === 'restart') {
      const room = rooms[myRoom];
      if (!room || room.hostId !== myId) return;
      room.started = false;
      room.secretHero = null;
      room.players.forEach(p => {
        p.ready = p.id === room.hostId;
        p.role = null;
        p.gameAssignment = null;
      });
      broadcast(myRoom, roomState(myRoom));
    }
  });

  ws.on('close', () => {
    if (!myRoom || !rooms[myRoom]) return;
    handleDisconnect(myRoom, myId);
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`Дота-Шпион запущен на порту ${PORT}`));
