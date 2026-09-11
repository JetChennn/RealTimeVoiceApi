import {STAGES, parseMetrics, metricDelta, parseScenes, base64Pcm, decodePcm, wavBlob} from './core.mjs';

const $ = id => document.getElementById(id);
const url = new URL('/v1/realtime', location.href);
url.protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
$('endpoint').textContent = `WebSocket：${url.href} · 指标：${location.origin}/metrics`;
$('connection').textContent = location.host;
const descriptions = {
  asr: ['ASR 识别', '转写调用 → 识别完成'], rag: ['RAG 检索', '检索排队 → 返回结果'],
  thinker: ['LLM 首包', '回复调用 → 首段文本'], tts: ['TTS 首音频', '合成调用 → 首块可发送音频'],
};
const cards = {};
for (const stage of STAGES) {
  const card = document.createElement('div'); card.className = 'metric';
  const label = document.createElement('div'); label.className = 'metric-name'; label.textContent = descriptions[stage][0];
  const value = document.createElement('div'); value.className = 'metric-value'; value.textContent = '—';
  const detail = document.createElement('div'); detail.className = 'metric-detail'; detail.textContent = descriptions[stage][1];
  card.append(label, value, detail); $('metrics').append(card); cards[stage] = {value, detail};
}
let active = null;
let turns = new Map();
let sequence = 0;
let totalTurns = 0;
function notice(text, error = false) { $('notice').textContent = text; $('notice').classList.toggle('error', error); }
function status(text, live = false) { $('status').textContent = text; $('status').classList.toggle('live', live); }
function lock(locked) {
  $('start').disabled = locked; $('cancel').disabled = !locked;
  $('device').disabled = locked; $('rag').disabled = locked;
  $('scenes').disabled = locked || !$('rag').checked;
}
$('rag').onchange = () => { $('scenes').disabled = !$('rag').checked; };
function stopPlayback(session) {
  if (!session) return;
  for (const source of session.sources) { try { source.stop(); } catch {} }
  session.sources.clear(); session.playAt = 0;
  document.querySelectorAll('audio').forEach(audio => audio.pause());
}
function stop(session = active, message = '已取消，连接已关闭', error = false) {
  if (!session || active !== session) return;
  active = null;
  clearTimeout(session.pollTimer); clearTimeout(session.connectTimer);
  session.fetchController?.abort();
  session.stream?.getTracks().forEach(track => track.stop());
  session.capture?.disconnect(); session.source?.disconnect();
  stopPlayback(session);
  session.context?.close().catch(() => {});
  if (session.socket && session.socket.readyState < WebSocket.CLOSING) {
    if (session.socket.readyState === WebSocket.OPEN) {
      session.socket.send(JSON.stringify({type: 'CLOSE_SESSION', session_id: session.id}));
    }
    session.socket.close(1000, 'test ended');
  }
  for (const turn of turns.values()) if (!turn.done) finishTurn(turn, '已取消');
  $('level').value = 0; $('mic-label').textContent = '麦克风已关闭';
  $('metrics-status').textContent = '采样已停止'; lock(false); status(error ? '连接异常' : '已结束'); notice(message, error);
}
function clearTurns() {
  for (const turn of turns.values()) if (turn.objectUrl) URL.revokeObjectURL(turn.objectUrl);
  turns = new Map(); totalTurns = 0; $('turns').replaceChildren(); $('empty').classList.remove('hidden'); $('turn-count').textContent = '0 轮';
}
function makeTurn(id) {
  if (turns.has(id)) return turns.get(id);
  const element = document.createElement('article'); element.className = 'turn';
  const heading = document.createElement('div'); heading.className = 'turn-heading';
  const label = document.createElement('span'); label.textContent = `第 ${id} 轮`;
  const state = document.createElement('span'); state.textContent = '等待回复'; heading.append(label, state);
  function row(title, className) {
    const row = document.createElement('div'); row.className = `utterance ${className}`;
    const label = document.createElement('label'); label.textContent = title;
    const text = document.createElement('p'); text.textContent = '…'; row.append(label, text); element.append(row); return text;
  }
  element.append(heading);
  const asr = row('识别', 'asr'), reply = row('回复', 'reply');
  const audioRow = document.createElement('div'); audioRow.className = 'audio-row';
  const audioNote = document.createElement('span'); audioNote.className = 'audio-note'; audioNote.textContent = '等待语音';
  audioRow.append(audioNote); element.append(audioRow);
  const turn = {id, element, asr, reply, state, audioRow, audioNote, text: '', chunks: [], audioBytes: 0, next: 0, rate: null, interrupted: false, done: false};
  turns.set(id, turn); $('turns').append(element); $('empty').classList.add('hidden');
  $('turn-count').textContent = `${++totalTurns} 轮 · 保留最近 50 轮`;
  // Remove completed records only; late terminal events still belong to their original turn.
  if (turns.size > 50) {
    for (const [key, old] of turns) {
      if (old.done && key !== id) {
        old.element.querySelector('audio')?.pause(); old.element.remove();
        if (old.objectUrl) URL.revokeObjectURL(old.objectUrl);
        turns.delete(key); break;
      }
    }
  }
  return turn;
}
function finishTurn(turn, label) {
  turn.done = true; turn.state.textContent = label;
  if (!turn.text) turn.reply.textContent = '本轮未生成回复';
  if (turn.objectUrl || !turn.chunks.length) {
    if (!turn.chunks.length && !turn.objectUrl) turn.audioNote.textContent = turn.interrupted ? '已打断，无可重播音频' : '无音频';
    return;
  }
  turn.objectUrl = URL.createObjectURL(wavBlob(turn.chunks, turn.rate));
  const player = document.createElement('audio'); player.controls = true; player.preload = 'none'; player.src = turn.objectUrl;
  player.setAttribute('aria-label', `第 ${turn.id} 轮回复音频`);
  player.addEventListener('play', () => {
    if (active) { for (const source of active.sources) { try { source.stop(); } catch {} } active.sources.clear(); active.playAt = 0; }
    document.querySelectorAll('audio').forEach(other => { if (other !== player) other.pause(); });
  });
  turn.audioRow.replaceChildren(player);
  turn.chunks = [];
}
function playChunk(session, turn, message) {
  if (turn.interrupted || message.interrupt) return;
  if (message.sequence !== turn.next || message.audio_format !== 'PCM16' || message.channels !== 1 || ![16000, 24000, 48000].includes(message.sample_rate)) throw new Error('下行音频格式或序号异常');
  if (turn.rate !== null && turn.rate !== message.sample_rate) throw new Error('同一轮音频采样率发生变化');
  const {bytes, samples} = decodePcm(message.audio_b64);
  turn.next++; turn.rate = message.sample_rate; turn.audioBytes += bytes.length;
  if (turn.audioBytes > turn.rate * 2 * 120) throw new Error('单轮音频超过 120 秒，请重新开始测试');
  turn.chunks.push(bytes); turn.audioNote.textContent = '正在自动播放，完成后可重播';
  document.querySelectorAll('audio').forEach(audio => audio.pause());
  const ctx = session.context;
  const buffer = ctx.createBuffer(1, samples.length, turn.rate); buffer.copyToChannel(samples, 0);
  const source = ctx.createBufferSource(); source.buffer = buffer; source.connect(ctx.destination);
  const startAt = Math.max(ctx.currentTime + .03, session.playAt || 0);
  if (startAt - ctx.currentTime > 60) throw new Error('播放积压超过 60 秒，请重新开始测试');
  source.onended = () => { session.sources.delete(source); source.disconnect(); };
  session.sources.add(source); source.start(startAt); session.playAt = startAt + buffer.duration;
}
function receive(session, message) {
  if (active !== session) return;
  if (message.type === 'ERROR') {
    const text = `${message.stage} / ${message.code}：${message.message}`;
    if (!message.recoverable) stop(session, text, true); else notice(text, true);
    return;
  }
  if (message.session_id !== session.id) throw new Error('收到其他会话的消息');
  if (message.type === 'SESSION_CREATED') {
    if (session.ready) throw new Error('重复收到创建确认');
    if (message.sample_rate !== session.context.sampleRate || message.channels !== 1 || message.audio_format !== 'PCM16') throw new Error('服务端协商的音频格式不一致');
    session.ready = true; clearTimeout(session.connectTimer);
    status('对话中', true); $('mic-label').textContent = '正在聆听';
    notice('正在持续录音。说话后稍作停顿，服务端会自动切句；点击取消结束。');
    return;
  }
  if (message.type === 'ASR_RESULT') {
    // Also stop buffered playback from already-completed turns on a new utterance.
    stopPlayback(session);
    makeTurn(message.turn_id).asr.textContent = message.text;
    return;
  }
  const turn = turns.get(message.turn_id);
  if (!turn) return;
  if (message.interrupt) { turn.interrupted = true; turn.element.classList.add('interrupted'); }
  switch (message.type) {
    case 'TURN_STATE':
      stopPlayback(session); turn.interrupted = true; turn.state.textContent = '已打断'; turn.element.classList.add('interrupted'); break;
    case 'TEXT_DELTA':
      turn.text += message.delta;
      if (turn.text.length > 100000) throw new Error('单轮回复过长，请重新开始测试');
      turn.reply.textContent = turn.text; if (!turn.interrupted) turn.state.textContent = '回复中'; break;
    case 'TEXT_END': turn.text = message.text; turn.reply.textContent = message.text; if (!turn.interrupted) turn.state.textContent = '语音合成中'; break;
    case 'AUDIO_DELTA': playChunk(session, turn, message); break;
    case 'RESPONSE_END': finishTurn(turn, {COMPLETED: '已完成', INTERRUPTED: '已打断', FAILED: '失败'}[message.status] || message.status); break;
  }
}
async function sampleMetrics(session) {
  session.fetchController = new AbortController();
  const timer = setTimeout(() => session.fetchController?.abort(), 4000);
  try {
    const response = await fetch('/metrics', {cache: 'no-store', signal: session.fetchController.signal});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const current = parseMetrics(await response.text());
    if (active !== session) return;
    if (session.baseline) {
      const {reset, samples} = metricDelta(session.baseline, current);
      if (reset) {
        for (const stage of STAGES) { cards[stage].value.textContent = '—'; cards[stage].detail.textContent = '计数重置，重新采样'; }
      } else {
        for (const stage of STAGES) if (samples[stage]) {
          const {ms, count} = samples[stage];
          cards[stage].value.replaceChildren(document.createTextNode(ms.toFixed(0)));
          const unit = document.createElement('small'); unit.textContent = 'ms'; cards[stage].value.append(unit);
          cards[stage].detail.textContent = `${count} 个新增样本均值 · ${new Date().toLocaleTimeString('zh-CN', {hour12: false})}`;
        }
      }
      $('metrics-status').textContent = reset ? '计数重置，已重新建立基线' : '每秒采样 · 无新样本时保留上次值';
    } else $('metrics-status').textContent = '基线已建立，等待新增样本';
    session.baseline = current;
  } catch (error) {
    if (active === session) $('metrics-status').textContent = `指标暂不可用：${error.message}；将自动重试`;
  } finally { clearTimeout(timer); }
}
async function poll(session) {
  await sampleMetrics(session);
  if (active === session) session.pollTimer = setTimeout(() => poll(session), 1000);
}
async function start() {
  if (active) return;
  let scenes;
  try {
    if (!window.isSecureContext || !navigator.mediaDevices?.getUserMedia) throw new Error('请通过 SSH 端口转发后，在 localhost 地址打开页面以使用麦克风');
    if (!$('device').value.trim()) throw new Error('请填写设备标识');
    scenes = $('rag').checked ? parseScenes($('scenes').value) : [];
  } catch (error) { notice(error.message, true); return; }
  const session = {id: crypto.randomUUID(), sources: new Set(), playAt: 0, ready: false};
  active = session; sequence = 0; clearTurns(); lock(true); status('正在连接');
  $('session-label').textContent = `会话：${session.id}`; notice('请允许浏览器使用麦克风。');
  for (const stage of STAGES) { cards[stage].value.textContent = '—'; cards[stage].detail.textContent = descriptions[stage][1]; }
  try {
    session.context = new AudioContext({sampleRate: 24000});
    await session.context.resume();
    if (active !== session) return;
    if (![16000, 24000, 48000].includes(session.context.sampleRate)) throw new Error('浏览器不支持本网关需要的录音采样率');
    const stream = await navigator.mediaDevices.getUserMedia({audio: {channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true}, video: false});
    if (active !== session) { stream.getTracks().forEach(track => track.stop()); return; }
    session.stream = stream;
    stream.getAudioTracks().forEach(track => track.addEventListener('ended', () => stop(session, '麦克风已断开，请重新开始', true)));
    await session.context.audioWorklet.addModule(new URL('./audio-worklet.js', import.meta.url));
    if (active !== session) return;
    session.capture = new AudioWorkletNode(session.context, 'pcm-capture');
    session.capture.onprocessorerror = () => stop(session, '麦克风音频处理失败，请重新开始', true);
    session.source = session.context.createMediaStreamSource(stream);
    session.source.connect(session.capture); session.capture.connect(session.context.destination);
    let sentSamples = 0;
    session.capture.port.onmessage = ({data}) => {
      if (active !== session) return;
      $('level').value = Math.min(1, data.rms * 7);
      if (!session.ready || session.socket.readyState !== WebSocket.OPEN) return;
      if (session.socket.bufferedAmount > session.context.sampleRate * 2 * 2) { stop(session, '网络发送积压，请检查连接后重试', true); return; }
      session.socket.send(JSON.stringify({type: 'AUDIO_CHUNK', session_id: session.id, sequence: sequence++, timestamp_ms: Math.floor(sentSamples / session.context.sampleRate * 1000), audio_b64: base64Pcm(data.pcm)}));
      sentSamples += data.pcm.byteLength / 2;
    };
    await sampleMetrics(session);
    if (active !== session) return;
    session.pollTimer = setTimeout(() => poll(session), 1000);
    const socket = new WebSocket(url); session.socket = socket;
    session.connectTimer = setTimeout(() => stop(session, '连接或会话创建超时，请检查网关', true), 12000);
    socket.onopen = () => {
      if (active !== session) { socket.close(); return; }
      socket.send(JSON.stringify({type: 'CREATE_SESSION', protocol_version: 1, device_id: $('device').value.trim(), session_id: session.id, audio_format: 'PCM16', audio_transport: 'BASE64_JSON', sample_rate: session.context.sampleRate, channels: 1, rag_enabled: $('rag').checked, scenes}));
    };
    socket.onmessage = event => { try { receive(session, JSON.parse(event.data)); } catch (error) { stop(session, `处理消息失败：${error.message}`, true); } };
    socket.onerror = () => stop(session, 'WebSocket 连接失败，请确认端口转发与网关状态', true);
    socket.onclose = () => stop(session, '网关已断开连接，录音已停止', true);
  } catch (error) {
    stop(session, error.name === 'NotAllowedError' ? '麦克风权限被拒绝，请在浏览器设置中允许后重试' : `无法开始：${error.message}`, true);
  }
}
$('start').onclick = start;
$('cancel').onclick = () => stop();
window.addEventListener('pagehide', () => { stop(); clearTurns(); });
