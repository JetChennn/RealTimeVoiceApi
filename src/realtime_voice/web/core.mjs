export const STAGES = ['asr', 'semantic', 'semantic_inference', 'semantic_queue', 'thinker', 'tts', 'vad_ready_to_first_tts', 'speech_end_to_first_tts_excluding_silence'];
const DIRECT_HISTOGRAMS = {
  vad_ready_to_first_tts: 'realtime_voice_vad_ready_to_first_tts_seconds',
  speech_end_to_first_tts_excluding_silence: 'realtime_voice_speech_end_to_first_tts_excluding_silence_seconds',
};
export function parseMetrics(text) {
  const values = Object.fromEntries(STAGES.map(stage => [stage, {sum: 0, count: 0}]));
  let found = false;
  for (const line of text.split('\n')) {
    const match = line.match(/^realtime_voice_stage_latency_seconds_(sum|count)\{([^}]+)\}\s+([^\s]+)/);
    if (!match) continue;
    const stage = match[2].match(/(?:^|,)\s*stage="([^"]+)"/)?.[1];
    const value = Number(match[3]);
    if (stage in values && Number.isFinite(value) && value >= 0) {
      values[stage][match[1]] = value;
      found = true;
    }
  }
  for (const [stage, metric] of Object.entries(DIRECT_HISTOGRAMS)) {
    const expression = new RegExp(`^${metric}_(sum|count)\\s+([^\\s]+)$`);
    for (const line of text.split('\n')) {
      const match = line.match(expression);
      if (!match) continue;
      const value = Number(match[2]);
      if (Number.isFinite(value) && value >= 0) {
        values[stage][match[1]] = value;
        found = true;
      }
    }
  }
  // A fresh process has no labelled stage samples yet.
  if (!found
    && !text.includes('# HELP realtime_voice_stage_latency_seconds ')
    && !text.includes('# HELP realtime_voice_speech_end_to_first_tts_excluding_silence_seconds ')) {
    throw new Error('响应不是本网关的阶段指标');
  }
  return values;
}
export function metricDelta(previous, current) {
  const reset = STAGES.some(stage => current[stage].count < previous[stage].count || current[stage].sum < previous[stage].sum);
  return {reset, samples: Object.fromEntries(STAGES.map(stage => {
    const count = current[stage].count - previous[stage].count;
    return [stage, !reset && count > 0 ? {count, ms: Math.max(0, (current[stage].sum - previous[stage].sum) / count * 1000)} : null];
  }))};
}
export function parseScenes(value) {
  const scenes = [...new Set(value.split(/[,，\n]/).map(s => s.trim()).filter(Boolean))];
  if (scenes.length > 3) throw new Error('最多填写 3 个知识场景');
  return scenes;
}
export function base64Pcm(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary);
}

export function isStaleAudioChunk(capturedAtMs, currentAudioTimeMs, maxAgeMs) {
  return Number.isFinite(capturedAtMs)
    && Number.isFinite(currentAudioTimeMs)
    && Number.isFinite(maxAgeMs)
    && maxAgeMs >= 0
    && currentAudioTimeMs - capturedAtMs > maxAgeMs;
}
export function shouldDropAudioDelta(turnInterrupted, messageInterrupt) {
  return Boolean(turnInterrupted || messageInterrupt);
}
export function decodePcm(base64) {
  const binary = atob(base64);
  if (!binary.length || binary.length % 2) throw new Error('收到不完整的 PCM16 音频');
  const bytes = Uint8Array.from(binary, c => c.charCodeAt(0));
  const view = new DataView(bytes.buffer);
  const samples = new Float32Array(bytes.length / 2);
  for (let i = 0; i < samples.length; i++) samples[i] = view.getInt16(i * 2, true) / 32768;
  return {bytes, samples};
}
export function wavBlob(chunks, sampleRate) {
  const size = chunks.reduce((total, chunk) => total + chunk.length, 0);
  const header = new ArrayBuffer(44);
  const view = new DataView(header);
  const write = (offset, text) => [...text].forEach((c, i) => view.setUint8(offset + i, c.charCodeAt(0)));
  write(0, 'RIFF'); view.setUint32(4, size + 36, true); write(8, 'WAVE'); write(12, 'fmt ');
  view.setUint32(16, 16, true); view.setUint16(20, 1, true); view.setUint16(22, 1, true);
  view.setUint32(24, sampleRate, true); view.setUint32(28, sampleRate * 2, true);
  view.setUint16(32, 2, true); view.setUint16(34, 16, true); write(36, 'data'); view.setUint32(40, size, true);
  return new Blob([header, ...chunks], {type: 'audio/wav'});
}

export const TURN_METRIC_LABELS = {
  asr: 'ASR 识别', semantic: '语义结束判断', llm: 'LLM 首包',
  tts: 'TTS 首包', total: '首音总耗时',
};
export const TIMING_STATUS = {
  pending: '等待中', completed: '已完成', end: '语义结束', wait: '语义未结束',
  not_run: '未执行', cancelled: '已取消', interrupted: '已打断', failed: '失败',
  timeout: '超时', not_produced: '未产生首包',
  overloaded: '过载', matched: '声纹匹配', unregistered: '未注册', disabled: '未启用',
  mismatch: '声纹不匹配', too_short: '音频时长不足', embedding_failed: '声纹提取失败',
};
export function formatTiming(value) {
  if (typeof value?.ms === 'number' && Number.isFinite(value.ms) && value.ms >= 0) return `${value.ms.toFixed(0)} ms`;
  return TIMING_STATUS[value?.status] || '等待中';
}
export function acceptTimingSnapshot(previous, next) {
  return Number.isInteger(next?.sequence) && next.sequence > (previous?.sequence || 0)
    && (!previous || previous.status === 'pending');
}
export function isNearChatBottom(viewport, threshold = 48) {
  return viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight <= threshold;
}

export function createChatScroller(viewport, button, schedule = requestAnimationFrame) {
  let following = true;
  let scheduled = false;
  let lastTop = viewport.scrollTop;
  const refresh = () => { button.classList.toggle('hidden', following); };
  viewport.addEventListener('scroll', () => {
    // Layout/scroll anchoring can emit scroll events as streamed content grows.
    // Only an upward movement away from the bottom suspends following.
    const nearBottom = isNearChatBottom(viewport);
    if (!following) following = nearBottom;
    else if (viewport.scrollTop < lastTop && !nearBottom) following = false;
    lastTop = viewport.scrollTop;
    refresh();
  }, {passive: true});
  const changed = () => {
    if (!following || scheduled) return;
    scheduled = true;
    schedule(() => {
      scheduled = false;
      if (following) {
        viewport.scrollTop = viewport.scrollHeight;
        lastTop = viewport.scrollTop;
      }
      refresh();
    });
  };
  button.addEventListener('click', () => { following = true; changed(); refresh(); });
  return {changed, reset() { following = true; changed(); refresh(); }};
}

export function createTimingPanel(document) {
  const element = document.createElement('section'); element.className = 'turn-timing';
  element.setAttribute('aria-label', '本轮耗时');
  const grid = document.createElement('div'); grid.className = 'turn-timing-grid';
  const values = {};
  for (const [key, label] of Object.entries(TURN_METRIC_LABELS)) {
    const card = document.createElement('div'); card.className = `turn-metric turn-metric-${key}`;
    const name = document.createElement('span'); name.textContent = label;
    const value = document.createElement('strong'); value.textContent = '等待中';
    card.append(name, value); grid.append(card); values[key] = value;
  }
  const waits = document.createElement('p'); waits.className = 'turn-waits';
  waits.textContent = '等待网关逐轮计时';
  const details = document.createElement('details');
  const summary = document.createElement('summary'); summary.textContent = '计时明细与口径';
  const note = document.createElement('p'); note.className = 'timing-note';
  note.textContent = '总耗时：最终 VAD 切段 → 网关首块可发送音频。阶段包含排队；LLM 与 TTS 可重叠，不能相加。主值对应最终语音段与版本。';
  const reason = document.createElement('p');
  const list = document.createElement('div'); list.className = 'timing-detail-list';
  details.append(summary, note, reason, list); element.append(grid, waits, details);
  return {element, values, waits, reason, list};
}

export function renderTurnTiming(document, panel, snapshot) {
  for (const key of Object.keys(TURN_METRIC_LABELS)) {
    panel.values[key].textContent = formatTiming(snapshot[key]);
    panel.values[key].title = TIMING_STATUS[snapshot[key]?.status] || snapshot[key]?.status || '';
  }
  const pre = formatTiming({ms: snapshot.pre_segment_silence_ms, status: 'not_run'});
  const post = formatTiming({ms: snapshot.post_segment_wait_ms, status: 'not_run'});
  panel.waits.textContent = `切段前静音 ${pre}（不计入总耗时） · 切段后纯等待 ${post}（已计入总耗时）`;
  const reasons = {vad: 'VAD 切段', semantic_end: '语义结束', max_silence: '静音兜底', max_utterance: '达到最长语音限制'};
  panel.reason.textContent = `提交原因：${reasons[snapshot.commit_reason] || snapshot.commit_reason || '—'}${snapshot.fallback ? ' · 使用兜底回复，兜底文本不作为 LLM 首包' : ''}`;
  panel.list.replaceChildren();
  for (const detail of snapshot.details || []) {
    const row = document.createElement('p');
    const label = TURN_METRIC_LABELS[detail.stage] || (detail.stage === 'speaker' ? '声纹预检' : detail.stage);
    const id = detail.segment_id ? ` · 语音段 ${detail.segment_id}` : detail.version != null ? ` · 版本 ${detail.version}` : '';
    const offset = Number.isFinite(detail.offset_ms) ? ` · 起点 ${detail.offset_ms.toFixed(0)} ms` : '';
    const queue = detail.queue_ms != null ? ` · 排队 ${detail.queue_ms.toFixed(0)} ms / 调用 ${detail.call_ms.toFixed(0)} ms` : '';
    row.textContent = `${label}${id}${offset}：${formatTiming(detail)}${queue} · ${TIMING_STATUS[detail.status] || detail.status}`;
    panel.list.append(row);
  }
}

export function finishTimingSnapshot(snapshot, status) {
  const next = {...snapshot, status};
  for (const key of Object.keys(TURN_METRIC_LABELS)) {
    if (!next[key] || next[key].status === 'pending') next[key] = {status: status === 'completed' ? 'not_produced' : status};
  }
  return next;
}
