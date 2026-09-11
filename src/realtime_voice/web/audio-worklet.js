/* Capture at the AudioContext's actual sample rate; emit 40 ms PCM16 chunks. */
class PcmCapture extends AudioWorkletProcessor {
  constructor() {
    super();
    this.buffer = new Int16Array(Math.round(sampleRate * .04));
    this.offset = 0;
    this.energy = 0;
  }
  process(inputs) {
    const channel = inputs[0]?.[0];
    if (!channel) return true;
    for (const value of channel) {
      const sample = Math.max(-1, Math.min(1, value));
      this.buffer[this.offset++] = Math.round(sample * (sample < 0 ? 32768 : 32767));
      this.energy += sample * sample;
      if (this.offset === this.buffer.length) {
        const pcm = new ArrayBuffer(this.buffer.length * 2);
        const view = new DataView(pcm);
        for (let i = 0; i < this.buffer.length; i++) view.setInt16(i * 2, this.buffer[i], true);
        this.port.postMessage({ pcm, rms: Math.sqrt(this.energy / this.offset) }, [pcm]);
        this.offset = 0;
        this.energy = 0;
      }
    }
    // Output stays silent: never route microphone audio to speakers.
    return true;
  }
}
registerProcessor('pcm-capture', PcmCapture);
