FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 \
      ffmpeg \
      ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY srt_relay.py /app/srt_relay.py

RUN mkdir -p /var/www/html/hls

EXPOSE 9000/udp 8088/tcp

CMD ["python3", "/app/srt_relay.py"]
