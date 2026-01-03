#!/usr/bin/env python3
"""
SRT Listener -> HLS Gateway with "Publish only when stable"

- ffmpeg always runs and writes PRIVATE HLS:
    /var/www/html/hls/stream.m3u8
- Script publishes PUBLIC playlist only when stable:
    /var/www/html/hls/live.m3u8

Ingestion pipeline must pull:
  http://<EC2_PUBLIC_IP>/hls/live.m3u8
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

PRIVATE_PLAYLIST = os.getenv("PRIVATE_PLAYLIST", "stream.m3u8")  # written by ffmpeg always
PUBLIC_PLAYLIST  = os.getenv("PUBLIC_PLAYLIST", "live.m3u8")    # only published when stable

# HLS behavior
HLS_TIME_SEC = int(os.getenv("HLS_TIME_SEC", "2"))
HLS_LIST_SIZE = int(os.getenv("HLS_LIST_SIZE", "6"))
HLS_DELETE_THRESHOLD = int(os.getenv("HLS_DELETE_THRESHOLD", "1"))

# Transcoding
VIDEO_CODEC = os.getenv("VIDEO_CODEC", "copy")  # "copy" or e.g. "libx264"
AUDIO_CODEC = os.getenv("AUDIO_CODEC", "aac")   # "copy", "aac", or "none"
X264_PRESET = os.getenv("X264_PRESET", "veryfast")
X264_TUNE = os.getenv("X264_TUNE", "zerolatency")
VIDEO_CRF = os.getenv("VIDEO_CRF", "23")
FORCE_KEYFRAMES = os.getenv("FORCE_KEYFRAMES", "true").lower() in ("1", "true", "yes")

# SRT tuning
SRT_LATENCY_US = int(os.getenv("SRT_LATENCY_US", "800000"))         # 800ms
SRT_RCVBUF_BYTES = int(os.getenv("SRT_RCVBUF_BYTES", "268435456"))  # 256MB
SRT_PKT_SIZE = int(os.getenv("SRT_PKT_SIZE", "1316"))

# Probe
INPUT_FORMAT = os.getenv("INPUT_FORMAT", "mpegts")
PROBE_SIZE = os.getenv("PROBE_SIZE", "5M")
ANALYZE_DURATION = os.getenv("ANALYZE_DURATION", "5M")

# Restart
RESTART_SLEEP_SEC = int(os.getenv("RESTART_SLEEP_SEC", "2"))

# Health
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8088"))
HEALTH_MAX_STALENESS_SEC = int(os.getenv("HEALTH_MAX_STALENESS_SEC", "10"))

# Gating (this is the key)
PUBLISH_STABLE_SECONDS = int(os.getenv("PUBLISH_STABLE_SECONDS", "8"))   # must be stable for N seconds
REQUIRE_SEGMENTS = int(os.getenv("REQUIRE_SEGMENTS", "2"))              # require at least N .ts files
FFPROBE_VALIDATE = os.getenv("FFPROBE_VALIDATE", "true").lower() in ("1", "true", "yes")

CLEANUP_ON_START = os.getenv("CLEANUP_ON_START", "true").lower() in ("1", "true", "yes")

STOP_EVENT = threading.Event()
PUBLISHED = False


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


def check_bins():
    if shutil.which("ffmpeg") is None:
        log("ERROR: ffmpeg not found.")
        sys.exit(1)
    if shutil.which("ffprobe") is None:
        log("ERROR: ffprobe not found.")
        sys.exit(1)


def private_playlist_path() -> str:
    return os.path.join(HLS_DIR, PRIVATE_PLAYLIST)


def public_playlist_path() -> str:
    return os.path.join(HLS_DIR, PUBLIC_PLAYLIST)


def list_segments() -> list[str]:
    try:
        return sorted([f for f in os.listdir(HLS_DIR) if f.endswith(".ts")])
    except Exception:
        return []


def ffprobe_ok(file_path: str) -> bool:
    if not FFPROBE_VALIDATE:
        return True
    try:
        # fast validation: just ensure demux + stream headers decode enough to identify streams
        cmd = [
            "ffprobe", "-hide_banner", "-loglevel", "error",
            "-show_entries", "stream=codec_type,codec_name",
            "-of", "default=nw=1",
            file_path
        ]
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=8)
        return r.returncode == 0 and ("codec_type=video" in r.stdout)
    except Exception:
        return False


def atomic_copy(src: str, dst: str):
    tmp = dst + ".tmp"
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)


def unpublish():
    global PUBLISHED
    p = public_playlist_path()
    try:
        if os.path.exists(p):
            os.remove(p)
    except Exception:
        pass
    if PUBLISHED:
        log("[PUBLISH] live.m3u8 removed (stream not stable).")
    PUBLISHED = False


def publish():
    global PUBLISHED
    src = private_playlist_path()
    dst = public_playlist_path()
    if not os.path.exists(src):
        return
    atomic_copy(src, dst)
    if not PUBLISHED:
        log("[PUBLISH] live.m3u8 is now AVAILABLE (stream stable).")
    PUBLISHED = True


# =========================
# ffmpeg command
# =========================
def build_ffmpeg_cmd() -> list[str]:
    srt_in = (
        f"srt://0.0.0.0:{INPUT_PORT}"
        f"?mode=listener"
        f"&transtype=live"
        f"&pkt_size={SRT_PKT_SIZE}"
        f"&latency={SRT_LATENCY_US}"
        f"&rcvbuf={SRT_RCVBUF_BYTES}"
    )

    seg_pattern = os.path.join(HLS_DIR, "seg_%06d.ts")
    hls_flags = "delete_segments+append_list+independent_segments+program_date_time+temp_file"

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-nostdin",
        "-fflags", "+genpts+discardcorrupt",
        "-err_detect", "ignore_err",
        "-analyzeduration", ANALYZE_DURATION,
        "-probesize", PROBE_SIZE,
        "-f", INPUT_FORMAT,
        "-i", srt_in,
    ]

    # Video
    if VIDEO_CODEC == "copy":
        cmd += ["-c:v", "copy"]
    else:
        cmd += ["-c:v", VIDEO_CODEC]
        if VIDEO_CODEC == "libx264":
            cmd += ["-preset", X264_PRESET, "-tune", X264_TUNE, "-pix_fmt", "yuv420p"]
            # repeat headers on every keyframe (helps HLS + decoder recovery)
            # make keyframes predictable for 2s segments (25fps -> keyint=50)
            keyint = max(25, int(25 * HLS_TIME_SEC))
            cmd += ["-x264-params", f"repeat-headers=1:keyint={keyint}:min-keyint={keyint}:scenecut=0"]
        if VIDEO_CRF:
            cmd += ["-crf", VIDEO_CRF]
        if FORCE_KEYFRAMES:
            cmd += ["-force_key_frames", f"expr:gte(t,n_forced*{HLS_TIME_SEC})"]

    # Audio
    ac = AUDIO_CODEC.lower()
    if ac in ("none", "disable", "disabled", "no"):
        cmd += ["-an"]
    elif ac == "copy":
        cmd += ["-c:a", "copy"]
    else:
        cmd += ["-c:a", AUDIO_CODEC, "-b:a", "128k", "-ar", "48000", "-ac", "2"]

    cmd += [
        "-f", "hls",
        "-hls_time", str(HLS_TIME_SEC),
        "-hls_list_size", str(HLS_LIST_SIZE),
        "-hls_delete_threshold", str(HLS_DELETE_THRESHOLD),
        "-hls_flags", hls_flags,
        "-hls_segment_filename", seg_pattern,
        private_playlist_path(),
    ]
    return cmd


# =========================
# Health endpoint
# =========================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/healthz", "/health", "/"):
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return

        priv = private_playlist_path()
        pub = public_playlist_path()

        priv_ok = False
        pub_ok = os.path.exists(pub)

        if os.path.exists(priv):
            try:
                age = time.time() - os.path.getmtime(priv)
                priv_ok = age <= HEALTH_MAX_STALENESS_SEC
            except Exception:
                priv_ok = False

        code = 200 if (priv_ok and pub_ok) else 503
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        body = {
            "private_ok": priv_ok,
            "public_published": pub_ok,
            "private_playlist": priv,
            "public_playlist": pub,
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
# Gatekeeper thread
# =========================
def gatekeeper_loop():
    """
    Publish live.m3u8 only after:
      - private playlist exists and is fresh
      - at least REQUIRE_SEGMENTS .ts files exist
      - optional ffprobe validation passes on the latest segment
      - conditions hold continuously for PUBLISH_STABLE_SECONDS
    """
    stable_since = None

    while not STOP_EVENT.is_set():
        priv = private_playlist_path()

        # Basic: private playlist fresh?
        priv_fresh = False
        if os.path.exists(priv):
            try:
                age = time.time() - os.path.getmtime(priv)
                priv_fresh = age <= HEALTH_MAX_STALENESS_SEC
            except Exception:
                priv_fresh = False

        segs = list_segments()
        enough_segs = len(segs) >= REQUIRE_SEGMENTS

        latest_ok = True
        if FFPROBE_VALIDATE and segs:
            latest_path = os.path.join(HLS_DIR, segs[-1])
            latest_ok = ffprobe_ok(latest_path)

        healthy = priv_fresh and enough_segs and latest_ok

        if healthy:
            if stable_since is None:
                stable_since = time.time()
            if time.time() - stable_since >= PUBLISH_STABLE_SECONDS:
                publish()
        else:
            stable_since = None
            unpublish()

        time.sleep(1)


# =========================
# Runner
# =========================
def run_forever():
    while not STOP_EVENT.is_set():
        unpublish()  # always start unpublished until stable again
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

    check_bins()
    ensure_dirs()
    if CLEANUP_ON_START:
        cleanup_hls_dir()

    health_server = start_health_server()

    # Start gatekeeper
    g = threading.Thread(target=gatekeeper_loop, daemon=True)
    g.start()

    public_ip = os.getenv("PUBLIC_IP", "<EC2_PUBLIC_IP_OR_DOMAIN>")
    log("=" * 70)
    log("SRT LISTENER -> HLS (PRIVATE) + PUBLISH-GATE (PUBLIC live.m3u8)")
    log("=" * 70)
    log(f"SRT INPUT (sender pushes):        srt://{public_ip}:{INPUT_PORT}?mode=caller")
    log(f"PRIVATE HLS (internal):           http://{public_ip}/hls/{PRIVATE_PLAYLIST}")
    log(f"PUBLIC HLS (use this in ingest):  http://{public_ip}/hls/{PUBLIC_PLAYLIST}")
    log("-" * 70)

    run_forever()

    try:
        health_server.shutdown()
    except Exception:
        pass
    log("Shutdown complete.")


if __name__ == "__main__":
    main()
