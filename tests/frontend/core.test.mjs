import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import {parseMetrics, metricDelta, parseScenes, decodePcm, base64Pcm, wavBlob} from '../../src/realtime_voice/web/core.mjs';
const empty = '# HELP realtime_voice_stage_latency_seconds Stage duration\n';
const metrics = (count, sum) => `${empty}realtime_voice_stage_latency_seconds_count{stage="rag"} ${count}\nrealtime_voice_stage_latency_seconds_sum{stage="rag"} ${sum}\n`;
test('metrics subtract cumulative histograms and distinguish resets and absent samples', () => {
  const baseline = parseMetrics(metrics(10, 2));
  const result = metricDelta(baseline, parseMetrics(metrics(12, 2.6)));
  assert.ok(Math.abs(result.samples.rag.ms - 300) < .0001);
  assert.equal(result.samples.rag.count, 2);
  assert.equal(result.samples.asr, null);
  assert.equal(metricDelta(baseline, parseMetrics(metrics(10, 2))).samples.rag, null);
  assert.equal(metricDelta(baseline, parseMetrics(metrics(1, .1))).reset, true);
  assert.equal(metricDelta(parseMetrics(empty), parseMetrics(metrics(1, .08))).samples.rag.ms, 80);
  assert.throws(() => parseMetrics('<html>not metrics</html>'));
});
test('scene input normalizes whitespace, duplicates and Chinese commas', () => {
  assert.deepEqual(parseScenes(' 农业社会，渔猎社会,农业社会 '), ['农业社会', '渔猎社会']);
  assert.throws(() => parseScenes(' , '));
  assert.throws(() => parseScenes('a,b,c,d'));
});
test('PCM16 roundtrip and WAV header preserve sample rate, signs and payload', async () => {
  const bytes = new Uint8Array([0, 128, 0, 0, 255, 127]);
  const decoded = decodePcm(base64Pcm(bytes.buffer));
  assert.deepEqual(decoded.bytes, bytes);
  assert.equal(decoded.samples[0], -1);
  assert.equal(decoded.samples[2], 32767 / 32768);
  assert.throws(() => decodePcm('AA=='));
  const result = await wavBlob([bytes], 24000).arrayBuffer();
  const view = new DataView(result);
  assert.equal(view.getUint32(24, true), 24000);
  assert.equal(view.getUint32(40, true), 6);
  assert.deepEqual(new Uint8Array(result, 44), bytes);
});
test('worklet emits continuous 40ms little-endian audio including silence', () => {
  let Processor;
  const messages = [];
  const sandbox = {sampleRate: 24000, AudioWorkletProcessor: class { constructor() { this.port = {postMessage: msg => messages.push(msg)}; } }, registerProcessor: (_, value) => { Processor = value; }};
  vm.runInNewContext(fs.readFileSync(new URL('../../src/realtime_voice/web/audio-worklet.js', import.meta.url), 'utf8'), sandbox);
  const capture = new Processor();
  for (let n = 0; n < 15; n++) capture.process([[new Float32Array(128).fill(n < 8 ? -.5 : 0)]]);
  assert.equal(messages.length, 2);
  assert.equal(messages[0].pcm.byteLength, 1920);
  assert.equal(new DataView(messages[0].pcm).getInt16(0, true), -16384);
  assert.equal(new DataView(messages[1].pcm).getInt16(1918, true), 0);
  assert.equal(capture.offset, 0);
});
