#!/usr/bin/env python3
import os, sys, time, signal, shutil, subprocess, threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

INPUT_PORT = int(os.getenv("INPUT_PORT", "9000"))

HLS_DIR = os.getenv("HLS_DIR", "/var/www/html/hls")
PLAYLIST_NAME = os.getenv("PLAYLIST_NAME", "stream.m3u8")

HLS_TIME_SEC = int(os.getenv("HLS_TIME_SEC", "2"))
HLS_LIST_SIZE = int(os.getenv("HLS_LIST_SIZE", "6"))
HLS_DELETE_THRESHOLD = int(os.getenv("HLS_DELETE_THRESHOLD", "1"))

# SRT tuning
SRT_LATENCY_US = int(os.getenv("SRT_LATENCY_US", "800000"))
SRT_RCVBUF_BYTES = int(os.getenv("SRT_RCVBUF_BYTES", "268435456"))
SRT_TRANSTYPE = os.getenv("SRT_TRANSTYPE", "live")
SRT_PKT_SIZE = int(os.getenv("SRT_PKT_SIZE", "1316"))

# Probe
PROBE_SIZE = os.getenv("PROBE_SIZE", "20M")
ANALYZE_DURATION = os.getenv("ANALYZE_DURATION", "20M")

# Local UDP hop (inside container)
UDP_HOST = os.getenv("UDP_HOST", "127.0.0.1")
UDP_PORT = int(os.getenv("UDP_PORT", "10000"))

# Video transcode settings (stage-1)
X264_PRESET = os.getenv("X264_PRESET", "veryfast")
X264_TUNE = os.getenv("X264_TUNE", "zerolatency")
VIDEO_BITRATE = os.getenv("VIDEO_BITRATE", "")   # e.g. 2500k
VIDEO_CRF = os.getenv("VIDEO_CRF", "23")         # good default

# Audio for HLS
AUDIO_CODEC = os.getenv("AUDIO_CODEC", "aac")    # aac safest
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "128k")

RESTART_SLEEP_SEC = int(os.getenv("RESTART_SLEEP_SEC", "2"))

HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8088"))
HEALTH_MAX_STALENESS_SEC = int(os.getenv("HEALTH_MAX_STALENESS_SEC", "10"))
CLEANUP_ON_START = os.getenv("CLEANUP_ON_START", "true").lower() in ("1", "true", "yes")

STOP_EVENT = threading.Event()

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
        log("ERROR: ffmpeg not found in PATH.")
        sys.exit(1)

def playlist_path():
    return os.path.join(HLS_DIR, PLAYLIST_NAME)

def build_srt_url():
    return (
        f"srt://0.0.0.0:{INPUT_PORT}"
        f"?mode=listener"
        f"&transtype={SRT_TRANSTYPE}"
        f"&pkt_size={SRT_PKT_SIZE}"
        f"&latency={SRT_LATENCY_US}"
        f"&rcvbuf={SRT_RCVBUF_BYTES}"
    )

def build_cmd_srt_to_udp():
    """
    Stage-1: SRT(mpegts) -> UDP(mpegts)
    We TRANSCODE VIDEO here so SPS/PPS is guaranteed (repeat-headers=1).
    """
    srt_in = build_srt_url()
    udp_out = f"udp://{UDP_HOST}:{UDP_PORT}?pkt_size=1316&buffer_size=6553600&fifo_size=5000000&overrun_nonfatal=1"

    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-nostdin",

        "-fflags", "+genpts+discardcorrupt",
        "-err_detect", "ignore_err",

        "-analyzeduration", ANALYZE_DURATION,
        "-probesize", PROBE_SIZE,

        "-f", "mpegts",
        "-i", srt_in,

        # video transcode (fix SPS/PPS forever)
        "-map", "0:v:0",
        "-map", "0:a?",
        "-c:v", "libx264",
        "-preset", X264_PRESET,
        "-tune", X264_TUNE,
        "-pix_fmt", "yuv420p",
        "-x264-params", "repeat-headers=1:keyint=60:min-keyint=60:scenecut=0",

        # control quality
    ]

    if VIDEO_BITRATE:
        cmd += ["-b:v", VIDEO_BITRATE]
    else:
        cmd += ["-crf", VIDEO_CRF]

    # audio: keep light; if source audio is weird, re-encode to AAC
    cmd += [
        "-c:a", "aac",
        "-b:a", AUDIO_BITRATE,
        "-ar", "48000",
        "-ac", "2",

        # mux to mpegts + resend headers
        "-f", "mpegts",
        "-mpegts_flags", "+resend_headers+initial_discontinuity",

        udp_out
    ]
    return cmd

def build_cmd_udp_to_hls():
    """
    Stage-2: UDP(mpegts) -> HLS
    Video is now clean H.264 (with repeated headers), so we COPY it.
    """
    udp_in = f"udp://{UDP_HOST}:{UDP_PORT}?fifo_size=5000000&overrun_nonfatal=1&timeout=5000000"

    seg_pattern = os.path.join(HLS_DIR, "seg_%Y%m%d_%H%M%S.ts")
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

        "-f", "mpegts",
        "-i", udp_in,

        "-map", "0:v:0",
        "-map", "0:a?",
        "-c:v", "copy",
    ]

    # audio for HLS
    if AUDIO_CODEC.lower() in ("none", "no", "disable"):
        cmd += ["-an"]
    elif AUDIO_CODEC.lower() == "copy":
        cmd += ["-c:a", "copy"]
    else:
        cmd += ["-c:a", "aac", "-b:a", AUDIO_BITRATE, "-ar", "48000", "-ac", "2"]

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

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/healthz", "/health", "/"):
            self.send_response(404); self.end_headers(); self.wfile.write(b"not found"); return

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
            "ok": ok, "playlist": p, "exists": exists,
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

def pump_logs(prefix, proc):
    assert proc.stdout is not None
    for line in proc.stdout:
        if STOP_EVENT.is_set():
            break
        log(f"{prefix} {line.rstrip()}")

def run_forever():
    while not STOP_EVENT.is_set():
        cmd1 = build_cmd_srt_to_udp()
        cmd2 = build_cmd_udp_to_hls()

        log("[SRT->UDP] starting ffmpeg:")
        log("  " + " ".join(cmd1))
        log("[UDP->HLS] starting ffmpeg:")
        log("  " + " ".join(cmd2))

        p1 = p2 = None
        try:
            p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            p2 = subprocess.Popen(cmd2, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

            t1 = threading.Thread(target=pump_logs, args=("[ffmpeg:SRT->UDP]", p1), daemon=True)
            t2 = threading.Thread(target=pump_logs, args=("[ffmpeg:UDP->HLS]", p2), daemon=True)
            t1.start(); t2.start()

            while not STOP_EVENT.is_set():
                rc1 = p1.poll()
                rc2 = p2.poll()
                if rc1 is not None or rc2 is not None:
                    log(f"[PIPELINE] exited (srt->udp={rc1}, udp->hls={rc2}). Restarting in {RESTART_SLEEP_SEC}s...")
                    break
                time.sleep(1)

        except Exception as e:
            log(f"[PIPELINE] ERROR: {e}. Restarting in {RESTART_SLEEP_SEC}s...")

        finally:
            for p in (p1, p2):
                try:
                    if p and p.poll() is None:
                        p.terminate()
                except Exception:
                    pass
            time.sleep(0.5)
            for p in (p1, p2):
                try:
                    if p and p.poll() is None:
                        p.kill()
                except Exception:
                    pass

        for _ in range(RESTART_SLEEP_SEC):
            if STOP_EVENT.is_set():
                break
            time.sleep(1)

    log("[PIPELINE] stopped.")

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
    log("SRT LISTENER  ->  UDP  ->  HLS (.m3u8) GATEWAY (VIDEO TRANSCODE FIX)")
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
