export const STAGES = ['asr', 'rag', 'thinker', 'tts'];
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
  // A fresh process has no labelled stage samples yet.
  if (!found && !text.includes('# HELP realtime_voice_stage_latency_seconds ')) {
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
  if (scenes.length < 1 || scenes.length > 3) throw new Error('开启 RAG 时请填写 1～3 个知识场景');
  return scenes;
}
export function base64Pcm(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary);
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
