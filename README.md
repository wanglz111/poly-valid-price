# poly-valid-price

采集并入库两类价格数据：
- Chainlink Streams（ETH/BTC/SOL）
- Polymarket 5m 市场 Up 买价（ETH/BTC/SOL）

## 项目文件
- `fetch_chainlink_reports.py`: Chainlink 价格采集并入库 MySQL
- `fetch_polymarket_prices.py`: Polymarket 价格采集并入库 MySQL（每秒写入）
- `docker-compose.yml`: 同时启动两个采集服务
- `.github/workflows/docker-build.yml`: GitHub Actions 自动构建并推送镜像到 GHCR

## 环境变量
1. 复制模板：
```bash
cp .env.example .env
```
2. 填写实际值（尤其 `MYSQL_PASSWORD`）

注意：
- `docker-compose.yml` 已强制 `MYSQL_HOST=db`
- 外部网络固定为 `poly_backend`

## 本地运行（Python）
安装依赖：
```bash
pip install -r requirements.txt
```

运行 Polymarket 采集：
```bash
python3 fetch_polymarket_prices.py
```

运行 Chainlink 采集：
```bash
python3 fetch_chainlink_reports.py
```

## Docker Compose 运行（两个服务同时跑）
确保外部网络存在：
```bash
docker network create poly_backend || true
```

启动：
```bash
docker compose up -d
```

查看日志：
```bash
docker compose logs -f
```

## GitHub Actions 自动打包
工作流：`.github/workflows/docker-build.yml`

触发条件：
- push 到 `main` / `master`
- push tag（`v*`）
- 手动触发（workflow_dispatch）

镜像地址：
- `ghcr.io/<owner>/<repo>:latest`
- `ghcr.io/<owner>/<repo>:sha-...`

## 隐私与安全
- `.env` 已被 `.gitignore` 和 `.dockerignore` 排除，不会被提交或打进镜像。
- 不要把真实密码写进 `.env.example`。
- 若密码曾泄露，先轮换数据库密码再部署。
