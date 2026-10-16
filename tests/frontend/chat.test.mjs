import test from 'node:test';
import assert from 'node:assert/strict';
import {formatTiming, acceptTimingSnapshot, finishTimingSnapshot, isNearChatBottom, createChatScroller, parseMetrics, metricDelta} from '../../src/realtime_voice/web/core.mjs';

test('latencies distinguish zero, pending, failed and absent first packets', () => {
  assert.equal(formatTiming({ms: 0}), '0 ms');
  assert.equal(formatTiming({ms: 800.01}), '800 ms');
  assert.equal(formatTiming({ms: null, status: 'pending'}), '等待中');
  assert.equal(formatTiming({status: 'interrupted'}), '已打断');
  assert.equal(formatTiming({status: 'not_produced'}), '未产生首包');
  assert.equal(formatTiming({ms: NaN, status: 'timeout'}), '超时');
});

test('snapshot updates reject older packets and changes after a terminal status', () => {
  const current = {sequence: 3, status: 'pending'};
  assert.equal(acceptTimingSnapshot(current, {sequence: 4, status: 'completed'}), true);
  assert.equal(acceptTimingSnapshot(current, {sequence: 3, status: 'pending'}), false);
  assert.equal(acceptTimingSnapshot(current, {sequence: 2, status: 'pending'}), false);
  assert.equal(acceptTimingSnapshot({sequence: 3, status: 'interrupted'}, {sequence: 4, status: 'pending'}), false);
  assert.equal(acceptTimingSnapshot(null, {sequence: 1, status: 'pending'}), true);
  assert.equal(acceptTimingSnapshot(null, {sequence: NaN}), false);
});

test('disconnect preserves measured values and marks unfinished fields', () => {
  const previous = {status: 'pending', asr: {ms: 100, status: 'completed'}, llm: {status: 'pending'}};
  const final = finishTimingSnapshot(previous, 'cancelled');
  assert.deepEqual(final.asr, previous.asr);
  assert.equal(final.llm.status, 'cancelled');
  assert.equal(final.total.status, 'cancelled');
  assert.equal(previous.llm.status, 'pending');
  assert.equal(finishTimingSnapshot(previous, 'completed').total.status, 'not_produced');
});

test('chat follows at bottom, pauses when reading history, and resumes on latest', () => {
  const events = {}, callbacks = [];
  const viewport = {scrollHeight: 1000, scrollTop: 600, clientHeight: 400,
    addEventListener(name, fn) { events[name] = fn; }};
  let hidden = true;
  const button = {classList: {toggle(_, value) { hidden = value; }},
    addEventListener(_, fn) { events.click = fn; }};
  const chat = createChatScroller(viewport, button, fn => callbacks.push(fn));
  assert.equal(isNearChatBottom(viewport), true);
  viewport.scrollHeight += 100;
  chat.changed(); chat.changed();
  assert.equal(callbacks.length, 1);
  callbacks.shift()();
  assert.equal(viewport.scrollTop, 1100);
  viewport.scrollTop = 100; events.scroll();
  assert.equal(hidden, false);
  viewport.scrollHeight += 100; chat.changed();
  assert.equal(callbacks.length, 0);
  assert.equal(viewport.scrollTop, 100);
  events.click(); callbacks.shift()();
  assert.equal(viewport.scrollTop, 1200);
  assert.equal(hidden, true);
  chat.reset(); callbacks.shift()();
  assert.equal(hidden, true);
});

test('a user scrolling up cancels a pending automatic scroll', () => {
  const events = {}, callbacks = [];
  const viewport = {scrollHeight: 1000, scrollTop: 600, clientHeight: 400,
    addEventListener(name, fn) { events[name] = fn; }};
  const button = {classList: {toggle() {}}, addEventListener() {}};
  const chat = createChatScroller(viewport, button, fn => callbacks.push(fn));
  chat.changed(); viewport.scrollTop = 100; events.scroll(); callbacks.shift()();
  assert.equal(viewport.scrollTop, 100);
});

test('new VAD-to-first-audio aggregate is parsed separately from legacy silence metric', () => {
  const metrics = (count, sum) => `realtime_voice_vad_ready_to_first_tts_seconds_count ${count}\nrealtime_voice_vad_ready_to_first_tts_seconds_sum ${sum}\n`;
  const delta = metricDelta(parseMetrics(metrics(1, 0.2)), parseMetrics(metrics(2, 1)));
  assert.equal(delta.samples.vad_ready_to_first_tts.ms, 800);
  assert.equal(delta.samples.speech_end_to_first_tts_excluding_silence, null);
});
