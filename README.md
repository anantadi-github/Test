# SRT Relay Server - Docker Deployment

## Quick Start

### Build and run the container:
```bash
docker-compose up -d
```

### View logs:
```bash
docker-compose logs -f
```

### Stop the container:
```bash
docker-compose down
```

### Restart the container:
```bash
docker-compose restart
```

## EC2 Deployment Instructions

1. **Install Docker on EC2:**
```bash
sudo apt update
sudo apt install -y docker.io docker-compose
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -a -G docker $USER
```

2. **Upload your files to EC2:**
```bash
scp -i your-key.pem srt_relay.py docker-compose.yml Dockerfile ubuntu@your-ec2-ip:/home/ubuntu/srt-relay/
```

3. **On EC2, start the service:**
```bash
cd ~/srt-relay
docker-compose up -d
```

Note: Docker auto-start is already enabled from step 1.

## Connection Details

- **Aterna Group connects to:** `srt://YOUR_EC2_IP:9000?mode=caller`
- **Viewers connect to:** `srt://YOUR_EC2_IP:9001?mode=caller`

## Monitoring

Check container status:
```bash
docker ps
docker stats srt-relay-server
```

View real-time logs:
```bash
docker logs -f srt-relay-server
```

## Auto-Restart

The container is configured with `restart: unless-stopped`, meaning it will:
- ✓ Automatically restart if it crashes
- ✓ Start automatically when EC2 reboots
- ✓ Keep running until manually stopped

## Security Group Settings (AWS EC2)

Make sure these ports are open in your EC2 Security Group:
- Port 9000 (UDP) - Inbound for Aterna
- Port 9001 (UDP) - Inbound for viewers
