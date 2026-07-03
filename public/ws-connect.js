function getWsUrl() {
  return `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}`;
}

// Render free tier "спит" ~15 мин — первое подключение может занять до минуты
function connectWithRetry({ onOpen, onMessage, onStatus, onDisconnect, maxAttempts = 12, retryDelay = 3000, timeout = 25000 }) {
  let ws;
  let attempt = 0;
  let stopped = false;
  let connected = false;

  function setStatus(text) {
    if (onStatus) onStatus(text);
  }

  function tryConnect() {
    if (stopped) return;
    attempt++;
    if (!connected) {
      setStatus(attempt === 1 ? 'Подключение...' : `Сервер просыпается... (${attempt}/${maxAttempts})`);
    }

    let opened = false;
    ws = new WebSocket(getWsUrl());
    const timer = setTimeout(() => { try { ws.close(); } catch {} }, timeout);

    ws.onopen = () => {
      opened = true;
      connected = true;
      clearTimeout(timer);
      attempt = 0;
      setStatus('');
      ws.onmessage = e => onMessage && onMessage(e, ws);
      ws.onclose = () => {
        if (stopped) return;
        if (onDisconnect) onDisconnect();
      };
      onOpen(ws);
    };

    ws.onclose = () => {
      clearTimeout(timer);
      if (stopped || opened) return;
      if (attempt < maxAttempts) {
        setTimeout(tryConnect, retryDelay);
      } else {
        setStatus('Не удалось подключиться. Подождите минуту и попробуйте снова.');
      }
    };
  }

  tryConnect();

  return {
    get socket() { return ws; },
    stop() { stopped = true; try { ws.close(); } catch {} },
  };
}
