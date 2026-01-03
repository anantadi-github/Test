#!/usr/bin/env python3
"""
SRT Listener -> HLS (m3u8) Gateway
---------------------------------
- Listens on SRT port (INPUT_PORT)
- Repackages to HLS (.m3u8 + .ts segments) in HLS_DIR
- nginx can serve /hls/stream.m3u8
- Auto-restarts ffmpeg if SRT drops / ffmpeg exits
- Optional lightweight health endpoint (HTTP) for monitoring

Sender (customer) pushes:
  srt://<EC2_PUBLIC_IP>:9000?mode=caller

Your ingestion pipeline pulls:
  http://<EC2_PUBLIC_IP>/hls/stream.m3u8
"""

import os
import sys
import time
import signal
import shlex
import shutil
import subprocess
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

# =========================
# Config
# =========================
INPUT_PORT = int(os.getenv("INPUT_PORT", "9000"))

HLS_DIR = os.getenv("HLS_DIR", "/var/www/html/hls")
PLAYLIST_NAME = os.getenv("PLAYLIST_NAME", "stream.m3u8")

# HLS behavior
HLS_TIME_SEC = int(os.getenv("HLS_TIME_SEC", "2"))         # segment duration
HLS_LIST_SIZE = int(os.getenv("HLS_LIST_SIZE", "6"))       # rolling window length
HLS_DELETE_THRESHOLD = int(os.getenv("HLS_DELETE_THRESHOLD", "1"))

# Transcoding / repackaging
# For gateway: prefer VIDEO_CODEC=copy (no decode) + AUDIO_CODEC=aac for HLS compatibility
VIDEO_CODEC = os.getenv("VIDEO_CODEC", "copy")  # "copy" or e.g. "libx264"
AUDIO_CODEC = os.getenv("AUDIO_CODEC", "aac")   # "copy", "aac", or "none"
X264_PRESET = os.getenv("X264_PRESET", "veryfast")
X264_TUNE = os.getenv("X264_TUNE", "zerolatency")
VIDEO_CRF = os.getenv("VIDEO_CRF", "")          # e.g. "23"
VIDEO_BITRATE = os.getenv("VIDEO_BITRATE", "")  # e.g. "2500k"
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")  # default for aac
FORCE_KEYFRAMES = os.getenv("FORCE_KEYFRAMES", "false").lower() in ("1", "true", "yes")

# SRT tuning (WAN-safe defaults)
SRT_LATENCY_US = int(os.getenv("SRT_LATENCY_US", "800000"))         # 800ms
SRT_RCVBUF_BYTES = int(os.getenv("SRT_RCVBUF_BYTES", "268435456"))  # 256MB

# Common for MPEG-TS over SRT (most encoders)
SRT_TRANSTYPE = os.getenv("SRT_TRANSTYPE", "live")   # live/file
SRT_PKT_SIZE = int(os.getenv("SRT_PKT_SIZE", "1316"))  # typical TS packet payload size

# Input probing/format override (use if ffmpeg mis-detects stream)
INPUT_FORMAT = os.getenv("INPUT_FORMAT", "mpegts")
PROBE_SIZE = os.getenv("PROBE_SIZE", "5M")
ANALYZE_DURATION = os.getenv("ANALYZE_DURATION", "5M")
FFMPEG_INPUT_ARGS = os.getenv("FFMPEG_INPUT_ARGS", "")

# Restart behavior
RESTART_SLEEP_SEC = int(os.getenv("RESTART_SLEEP_SEC", "2"))

# Optional health endpoint
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8088"))
HEALTH_MAX_STALENESS_SEC = int(os.getenv("HEALTH_MAX_STALENESS_SEC", "10"))

# Cleanup on start
CLEANUP_ON_START = os.getenv("CLEANUP_ON_START", "true").lower() in ("1", "true", "yes")

STOP_EVENT = threading.Event()


# =========================
# Utilities
# =========================
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def ensure_dirs():
    os.makedirs(HLS_DIR, exist_ok=True)


def cleanup_hls_dir():
    if not os.path.isdir(HLS_DIR):
        return
    for f in os.listdir(HLS_DIR):
        if f.endswith(".ts") or f.endswith(".m3u8") or f.endswith(".tmp"):
            try:
                os.remove(os.path.join(HLS_DIR, f))
            except Exception:
                pass


def check_ffmpeg():
    if shutil.which("ffmpeg") is None:
        log("ERROR: ffmpeg not found in PATH. Install it first.")
        sys.exit(1)


def playlist_path() -> str:
    return os.path.join(HLS_DIR, PLAYLIST_NAME)


def build_input_args() -> list[str]:
    args: list[str] = []
    if ANALYZE_DURATION:
        args += ["-analyzeduration", str(ANALYZE_DURATION)]
    if PROBE_SIZE:
        args += ["-probesize", str(PROBE_SIZE)]
    if INPUT_FORMAT:
        args += ["-f", INPUT_FORMAT]
    if FFMPEG_INPUT_ARGS:
        args += shlex.split(FFMPEG_INPUT_ARGS)
    return args


def build_codec_args() -> list[str]:
    args: list[str] = []

    # Video
    if VIDEO_CODEC == "copy":
        args += ["-c:v", "copy"]
    else:
        args += ["-c:v", VIDEO_CODEC]
        if VIDEO_CODEC == "libx264":
            args += ["-preset", X264_PRESET, "-tune", X264_TUNE]
            # Repeat SPS/PPS in output (good for HLS robustness)
            args += ["-x264-params", "repeat-headers=1"]
        if VIDEO_CODEC in ("libx264", "libx265") and VIDEO_CRF:
            args += ["-crf", VIDEO_CRF]
        if VIDEO_BITRATE:
            args += ["-b:v", VIDEO_BITRATE]
        if FORCE_KEYFRAMES:
            # align keyframes to segment duration
            args += ["-force_key_frames", f"expr:gte(t,n_forced*{HLS_TIME_SEC})"]

    # Audio
    audio_codec = AUDIO_CODEC.lower()
    if audio_codec in ("none", "disable", "disabled", "no"):
        args += ["-an"]
    elif audio_codec == "copy":
        args += ["-c:a", "copy"]
    else:
        args += ["-c:a", AUDIO_CODEC]
        if AUDIO_BITRATE:
            args += ["-b:a", AUDIO_BITRATE]
        # These help some HLS players; harmless if input already matches
        args += ["-ar", "48000", "-ac", "2"]

    return args


def build_ffmpeg_cmd() -> list[str]:
    """
    Repackage (or transcode) SRT listener -> HLS
    """
    # SRT listener URL
    srt_in = (
        f"srt://0.0.0.0:{INPUT_PORT}"
        f"?mode=listener"
        f"&transtype={SRT_TRANSTYPE}"
        f"&pkt_size={SRT_PKT_SIZE}"
        f"&latency={SRT_LATENCY_US}"
        f"&rcvbuf={SRT_RCVBUF_BYTES}"
    )

    seg_pattern = os.path.join(HLS_DIR, "seg_%Y%m%d_%H%M%S.ts")

    # HLS flags:
    # - delete_segments: disk remains bounded
    # - append_list: stable rolling playlist updates
    # - independent_segments: better seeking/robustness
    # - program_date_time: useful timestamps
    # - temp_file: atomic-ish writes
    hls_flags = "delete_segments+append_list+independent_segments+program_date_time+temp_file"

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",

        # tolerate imperfect live inputs
        "-fflags", "+genpts+discardcorrupt",
        "-err_detect", "ignore_err",

        # avoid stdin blocking in containers
        "-nostdin",
    ]

    cmd += build_input_args()
    cmd += ["-i", srt_in]

    # If stream contains multiple tracks, map first video + optional audio
    cmd += ["-map", "0:v:0", "-map", "0:a?"]

    cmd += build_codec_args()
    cmd += [
        "-f", "hls",
        "-hls_time", str(HLS_TIME_SEC),
        "-hls_list_size", str(HLS_LIST_SIZE),
        "-hls_delete_threshold", str(HLS_DELETE_THRESHOLD),
        "-hls_flags", hls_flags,
        "-hls_segment_filename", seg_pattern,
        playlist_path(),
    ]
    return cmd


# =========================
# Health Server
# =========================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/healthz", "/health", "/"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return

        p = playlist_path()
        ok = False
        age = None
        exists = os.path.exists(p)
        if exists:
            try:
                age = time.time() - os.path.getmtime(p)
                ok = age <= HEALTH_MAX_STALENESS_SEC
            except Exception:
                ok = False

        self.send_response(200 if ok else 503)
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        body = {
            "ok": ok,
            "playlist": p,
            "exists": exists,
            "playlist_age_sec": None if age is None else round(age, 3),
            "max_staleness_sec": HEALTH_MAX_STALENESS_SEC,
        }
        self.wfile.write((str(body).replace("'", '"')).encode("utf-8"))

    def log_message(self, format, *args):
        return


def start_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log(f"[HEALTH] listening on http://0.0.0.0:{HEALTH_PORT}/healthz")
    return server


# =========================
# Runner
# =========================
def run_forever():
    while not STOP_EVENT.is_set():
        cmd = build_ffmpeg_cmd()
        log("[SRT->HLS] starting ffmpeg:")
        log("  " + " ".join(cmd))

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            assert proc.stdout is not None
            for line in proc.stdout:
                if STOP_EVENT.is_set():
                    break
                log(f"[ffmpeg] {line.rstrip()}")

            if STOP_EVENT.is_set():
                break

            rc = proc.wait()
            log(f"[SRT->HLS] ffmpeg exited (code={rc}). Restarting in {RESTART_SLEEP_SEC}s...")

        except Exception as e:
            log(f"[SRT->HLS] ERROR: {e}. Restarting in {RESTART_SLEEP_SEC}s...")

        for _ in range(RESTART_SLEEP_SEC):
            if STOP_EVENT.is_set():
                break
            time.sleep(1)

    log("[SRT->HLS] stopped.")


def handle_signal(signum, frame):
    log("Signal received, stopping...")
    STOP_EVENT.set()


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    check_ffmpeg()
    ensure_dirs()
    if CLEANUP_ON_START:
        cleanup_hls_dir()

    health_server = start_health_server()

    public_ip = os.getenv("PUBLIC_IP", "<EC2_PUBLIC_IP_OR_DOMAIN>")
    log("=" * 70)
    log("SRT LISTENER  ->  HLS (.m3u8) GATEWAY")
    log("=" * 70)
    log(f"SRT INPUT (sender pushes):   srt://{public_ip}:{INPUT_PORT}?mode=caller")
    log(f"HLS OUTPUT (ingestion pulls): http://{public_ip}/hls/{PLAYLIST_NAME}")
    log(f"HLS folder: {HLS_DIR}")
    log("-" * 70)

    run_forever()

    try:
        health_server.shutdown()
    except Exception:
        pass
    log("Shutdown complete.")


if __name__ == "__main__":
    main()
