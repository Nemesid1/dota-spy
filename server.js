const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PUBLIC = path.join(__dirname, 'public');

const MIME = {
  '.html': 'text/html',
  '.css':  'text/css',
  '.js':   'application/javascript',
};

// ── HTTP ──────────────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  let filePath = path.join(PUBLIC, req.url === '/' ? 'index.html' : req.url);
  const ext = path.extname(filePath);
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'text/plain' });
    res.end(data);
  });
});

// ── ROOMS ─────────────────────────────────────────────────────────────────────
// rooms: { code -> { hostId, players:[{id,name,ready,role,usedVote}], settings, started, vote } }
// vote: { initiatorId, targetId, targetName, votes:{id->bool}, resolved }
const rooms = {};
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
function roomState(roomCode) {
  const room = rooms[roomCode];
  return {
    type: 'room_state',
    code: roomCode,
    hostId: room.hostId,
    players: room.players.map(p => ({ id: p.id, name: p.name, ready: p.ready })),
    settings: room.settings,
    started: room.started,
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

// ── WS ───────────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server });

wss.on('connection', ws => {
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
      if (!room) { ws.send(JSON.stringify({ type: 'error', text: 'Комната не найдена' })); return; }
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
      if (!room) { ws.send(JSON.stringify({ type: 'error', text: 'Комната не найдена' })); return; }
      const existing = room.players.find(p => p.id === msg.playerId);
      if (existing) {
        // restore ws reference
        existing.ws = ws;
	clearTimeout(room._emptyTimer);
        myRoom = code;
        myId   = existing.id;
        ws.send(JSON.stringify({ type: 'joined', code, playerId: myId }));
        broadcast(code, roomState(code));
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

      // pick a random hero from disabled list sent by host
      const { heroes, disabledHeroes = [] } = msg;
      const available = heroes.filter(h => !disabledHeroes.includes(h.name));
      if (available.length === 0) {
        ws.send(JSON.stringify({ type: 'error', text: 'Нет доступных персонажей!' }));
        return;
      }
      const hero = available[Math.floor(Math.random() * available.length)];

      const n = room.players.length;
      const spyCount = Math.min(room.settings.spies, n - 1);

      // shuffle spy indices
      const indices = [...Array(n).keys()].sort(() => Math.random() - .5);
      const spyIds = new Set(indices.slice(0, spyCount).map(i => room.players[i].id));

      // assign random heroes to civilians (no repeats)
      const heroPool = [...available].sort(() => Math.random() - .5);
      let hi = 0;

      room.started = true;
      room.players.forEach(p => {
        const isSpy = spyIds.has(p.id);
        p.role = isSpy ? 'spy' : 'civilian';
        p.usedVote = false;
        const assignment = {
          type: 'game_start',
          role: isSpy ? 'spy' : 'civilian',
          hero: isSpy ? null : heroPool[hi++ % heroPool.length],
          location: hero,
          time: room.settings.time,
          players: room.players.map(x => ({ id: x.id, name: x.name })),
          myId: p.id,
        };
        if (p.ws && p.ws.readyState === 1) p.ws.send(JSON.stringify(assignment));
      });
      room.vote = null;
    }

    // ── timer sync (host broadcasts to all)
    else if (msg.type === 'timer') {
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

    // ── restart
    else if (msg.type === 'restart') {
      const room = rooms[myRoom];
      if (!room || room.hostId !== myId) return;
      room.started = false;
      room.players.forEach(p => p.ready = p.id === room.hostId);
      broadcast(myRoom, roomState(myRoom));
    }
  });

  ws.on('close', () => {
    if (!myRoom || !rooms[myRoom]) return;
    const room = rooms[myRoom];
    
    // Не удаляем игрока из списка! Просто обнуляем его вебсокет.
    const player = room.players.find(p => p.id === myId);
    if (player) player.ws = null;
    
    // Считаем, кто остался на связи
    const activePlayers = room.players.filter(p => p.ws !== null);
    
    // Если никого на связи нет, НЕ удаляем комнату сразу.
    // Даем 15 секунд на перезагрузку страницы (rejoin).
    if (activePlayers.length === 0) {
      clearTimeout(room._emptyTimer);
      room._emptyTimer = setTimeout(() => {
        // Если через 15 секунд так и никого нет — тогда удаляем
        if (rooms[myRoom] && rooms[myRoom].players.filter(p => p.ws !== null).length === 0) {
          delete rooms[myRoom];
        }
      }, 15000);
      return;
    }
    
    // Передача хоста, если хост вышел (но остались другие)
    if (room.hostId === myId) {
      room.hostId = activePlayers[0].id;
      activePlayers[0].ready = true;
    }
    broadcast(myRoom, roomState(myRoom));
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log(`Дота-Шпион запущен на порту ${PORT}`));
