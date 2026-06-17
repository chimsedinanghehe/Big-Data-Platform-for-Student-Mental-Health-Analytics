# MindSchool - Big Data Platform for Student Mental Health Analytics

## 1. Giới thiệu dự án

MindSchool là nền tảng phân tích sức khỏe tinh thần cho học sinh và sinh viên. Hệ thống cho phép người dùng làm khảo sát, trò chuyện với chatbot hỗ trợ tâm lý, sau đó dữ liệu được xử lý để hiển thị trên dashboard phân tích.

Mục tiêu của hệ thống:

- thu thập dữ liệu khảo sát sức khỏe tinh thần
- hỗ trợ người dùng bằng chatbot
- ghi nhận chatlog phục vụ phân tích
- xử lý dữ liệu lớn bằng Spark
- hiển thị thống kê trên dashboard
- tách phần web và phần Big Data để dễ mở rộng

## 2. Kiến trúc triển khai

MindSchool dùng kiến trúc hybrid:

```text
VPS chạy Web/API/Dashboard
Google Cloud chạy Data Lake và Spark batch
Kafka VM xử lý streaming chatlog
```

Các thành phần chính:

```text
Frontend        React web app
Backend         FastAPI service
Dashboard       Streamlit dashboard
Database        PostgreSQL
Gateway         Nginx reverse proxy
Container       Docker Compose
Streaming       Kafka
Data Lake       Google Cloud Storage
Batch           Dataproc Serverless Spark
Scheduler       Cloud Scheduler + Workflows
Vector DB       Qdrant
AI/RAG          OpenAI + Retrieval Augmented Generation
```

Luồng triển khai tổng quát:

```text
User
  -> Nginx HTTPS
  -> Frontend / Backend / Dashboard
  -> PostgreSQL
  -> Kafka / GCS
  -> Spark Batch
  -> Gold Tables
  -> Dashboard
```

## 3. Các tầng trong hệ thống

Hệ thống được chia thành 5 tầng:

```text
Tầng 1: Public Gateway
  Nginx nhận traffic HTTP/HTTPS.

Tầng 2: Application Runtime
  Docker Compose chạy frontend, backend, dashboard, PostgreSQL, worker.

Tầng 3: Streaming Ingestion
  Backend gửi chatlog vào Kafka, consumer ghi xuống GCS.

Tầng 4: Data Lake
  GCS lưu dữ liệu Bronze, Silver, Gold.

Tầng 5: Batch Analytics
  Cloud Scheduler gọi Workflow, Workflow chạy Spark batch trên Dataproc.
```

## 4. Public Gateway và bảo mật

Nginx chạy trực tiếp trên VPS và là cổng public duy nhất.

Nhiệm vụ của Nginx:

- nhận request từ `mindschool.site`
- redirect HTTP sang HTTPS
- cấp SSL bằng Let's Encrypt
- reverse proxy request vào container nội bộ

Routing production:

```text
/              -> frontend  -> 127.0.0.1:18080
/api/          -> backend   -> 127.0.0.1:18000
/ready         -> backend   -> 127.0.0.1:18000
/docs          -> backend   -> 127.0.0.1:18000
/dashboard/    -> dashboard -> 127.0.0.1:18501
```

Port public:

```text
80/tcp    HTTP + Certbot
443/tcp   HTTPS
22/tcp    SSH quản trị
```

Port nội bộ:

```text
Frontend   127.0.0.1:18080
Backend    127.0.0.1:18000
Dashboard  127.0.0.1:18501
```

Ý nghĩa bảo mật:

- container không expose trực tiếp ra Internet
- PostgreSQL không mở public port
- secret không đưa vào Docker image
- Kafka broker không mở public, backend đi qua SSH tunnel

## 5. Docker Compose Runtime

Production dùng 2 file Docker Compose:

```text
deploy/docker-compose.nginx-host.yml
deploy/docker-compose.nginx-host.bigdata.yml
```

Service chính:

```text
postgres
  Lưu user, session, profile, survey.

backend
  FastAPI, xử lý auth, survey, chatbot, RAG, Kafka, GCS readiness.

frontend
  React app đã build static.

dashboard
  Streamlit dashboard, đọc dữ liệu Gold từ GCS.

survey-snapshot-worker
  Đọc survey từ PostgreSQL và ghi snapshot lên GCS Bronze.
```

Keyword:

```text
Docker Compose
FastAPI
React
Streamlit
PostgreSQL
Worker
```

## 6. Kiến trúc Chatbot và RAG

Chatbot dùng RAG để trả lời dựa trên tài liệu đã được xử lý trước.

Luồng chatbot:

```text
User nhập câu hỏi
  -> Frontend gửi /api/rag/ask
  -> Backend rewrite query
  -> Truy xuất Qdrant bằng vector search
  -> Lấy top-k chunk liên quan
  -> Gọi LLM tạo câu trả lời
  -> Trả kết quả về frontend
  -> Ghi chatlog sang Kafka
```

Thành phần liên quan:

```text
OpenAI       sinh câu trả lời
Qdrant       lưu vector embedding
GCS          lưu tài liệu/chunk
Backend RAG  điều phối truy xuất và trả lời
Kafka        nhận chatlog để phân tích
```

Keyword:

```text
RAG
Vector Search
Embedding
Qdrant
Top-k Chunks
LLM
Chatlog
```

## 7. Kiến trúc Streaming Chatlog

Chatlog đi theo luồng streaming để dữ liệu chat được ghi nhanh xuống Data Lake.

Luồng chatlog:

```text
Backend
  -> Kafka topic student-chat-logs
  -> Kafka consumer
  -> GCS Silver
  -> Spark batch
  -> Gold tables
  -> Dashboard
```

Thông tin chính:

```text
Kafka topic        student-chat-logs
Output layer       GCS Silver
Output path        silver/anonymized_chat/date=YYYY-MM-DD/hour=HH/
Timezone           Asia/Ho_Chi_Minh
Format             Parquet
```

Điểm quan trọng:

- backend chỉ publish event, không xử lý batch nặng
- Kafka consumer ghi Parquet xuống GCS
- dữ liệu được partition theo ngày/giờ
- message lỗi đi vào DLQ
- offset chỉ commit sau khi upload GCS thành công

Keyword:

```text
Kafka
Streaming
Consumer
Parquet
Silver Layer
DLQ
At-least-once
```

## 8. Kiến trúc Survey Pipeline

Survey dùng mô hình snapshot + batch.

Luồng survey:

```text
Frontend gửi survey
  -> Backend lưu PostgreSQL
  -> survey-snapshot-worker đọc PostgreSQL
  -> Ghi GCS Bronze
  -> Spark xử lý Bronze -> Silver
  -> Spark xử lý Silver -> Gold
  -> Dashboard đọc Gold
```

Đường dẫn dữ liệu:

```text
Bronze:
gs://student-mental-health-lake-nhom1-2026/bronze/app_survey_snapshot/survey_all.parquet

Silver:
gs://student-mental-health-lake-nhom1-2026/silver/survey_cleaned/

Gold:
gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/
```

Lý do dùng batch:

- survey có nhiều câu hỏi và nhiều cột phân tích
- cần chuẩn hóa schema
- cần tổng hợp theo nhóm học sinh/sinh viên
- Spark phù hợp cho dữ liệu lớn

Keyword:

```text
Survey Snapshot
PostgreSQL
Bronze
Silver
Gold
Spark Batch
Dashboard Tables
```

## 9. Batch Analytics trên Google Cloud

Batch không chạy bằng cron local mà chạy trên Google Cloud.

Luồng batch:

```text
Cloud Scheduler
  -> Workflows
  -> Dataproc Serverless Spark
  -> GCS Silver/Gold
```

Thông tin lịch chạy:

```text
Scheduler name      nightly-dashboard-refresh-0000-vn
Workflow name       nightly-dashboard-refresh
Cron                0 0 * * *
Timezone            Asia/Ho_Chi_Minh
Process date        ngày hôm trước
```

Các batch chính:

```text
survey_bronze_to_silver
survey_silver_to_gold
chat_silver_to_gold
```

Keyword:

```text
Cloud Scheduler
Workflows
Dataproc Serverless
Spark Batch
Nightly Job
Gold Refresh
```

## 10. Dashboard và Cache

Dashboard dùng Streamlit và đọc dữ liệu đã xử lý ở Gold.

Dashboard không đọc raw data trực tiếp.

Luồng dashboard:

```text
GCS Gold Tables
  -> Dashboard cache
  -> Streamlit UI
  -> Researcher xem thống kê
```

Cache dashboard:

```text
cache_volume   dashboard_cache
cache_path     /app/data/.dashboard_cache
```

Mục tiêu cache:

- mở dashboard nhanh hơn
- giảm số lần đọc GCS
- tránh tính toán lại raw data
- hiển thị dữ liệu Gold mới sau batch

Keyword:

```text
Streamlit
Gold Tables
Dashboard Cache
Analytics UI
```

## 11. Luồng dữ liệu tổng quan

### 11.1 Luồng Web

```text
Frontend
  -> Backend API
  -> PostgreSQL
```

Backend xử lý:

- đăng ký
- đăng nhập
- phân quyền
- survey
- chatbot
- ghi log chat

### 11.2 Luồng Chat

```text
Backend
  -> Kafka
  -> GCS Silver
  -> Spark
  -> GCS Gold
  -> Dashboard
```

Gold output của chat:

```text
chat_hourly_metrics
chat_risk_summary
chat_topic_summary
chat_construct_summary
chat_model_usage
chat_sentiment_summary
```

### 11.3 Luồng Survey

```text
PostgreSQL
  -> Survey Worker
  -> GCS Bronze
  -> Spark Silver
  -> Spark Gold
  -> Dashboard
```

Gold output của survey:

```text
survey_overview_summary
survey_response_by_date
survey_demographic_summary
survey_analytic_features
```

### 11.4 Luồng RAG

```text
Tài liệu nguồn trên GCS
  -> Làm sạch
  -> Chia chunk
  -> Tạo embedding
  -> Lưu Qdrant
  -> Backend truy xuất khi user hỏi
```

## 12. Cách chạy dự án

### 12.1 Yêu cầu trước khi chạy

Máy chủ cần có:

```text
Ubuntu Server
Docker + Docker Compose v2
Nginx
Certbot
Git
Google Cloud CLI
```

Domain cần trỏ về IP máy chủ:

```text
mindschool.site       -> SERVER_PUBLIC_IP
www.mindschool.site   -> SERVER_PUBLIC_IP
```

### 12.2 Clone source code

```bash
sudo mkdir -p /opt/mindschool
sudo chown "$USER:$USER" /opt/mindschool
git clone <REPOSITORY_URL> /opt/mindschool/app
cd /opt/mindschool/app
```

### 12.3 Tạo file môi trường

```bash
cp deploy/.env.production.example deploy/.env.production
```

Các biến quan trọng:

```text
APP_DOMAIN=mindschool.site
ACME_EMAIL=<email>

POSTGRES_DB=student_mental_health_app
POSTGRES_USER=student_app
POSTGRES_PASSWORD=<password>

OPENAI_API_KEY=<openai_api_key>
OPENAI_MODEL=<model>

QDRANT_URL=<qdrant_url>
QDRANT_API_KEY=<qdrant_api_key>
QDRANT_COLLECTION=<collection>

GCP_PROJECT_ID=student-mental-health-496205
GCP_REGION=asia-southeast1
GCS_BUCKET_NAME=student-mental-health-lake-nhom1-2026

BIGDATA_ENABLED=true
READINESS_REQUIRE_GCS=true
READINESS_REQUIRE_KAFKA=true

CHAT_KAFKA_ENABLED=true
CHAT_KAFKA_TOPIC=student-chat-logs
```

Không commit file `.env.production`.

### 12.4 Chuẩn bị secret

GCP credential:

```text
deploy/secrets/gcp-service-account.json
```

Kafka SSH key:

```text
deploy/secrets/kafka-vm-ssh-key
```

Phân quyền:

```bash
chmod 600 deploy/secrets/gcp-service-account.json
chmod 600 deploy/secrets/kafka-vm-ssh-key
```

### 12.5 Cài Nginx, Docker, Certbot

```bash
sudo APP_ROOT=/opt/mindschool/app \
  APP_DOMAIN=mindschool.site \
  ACME_EMAIL=<email> \
  bash deploy/install-nginx-certbot.sh
```

Kiểm tra:

```bash
sudo nginx -t
sudo systemctl status nginx --no-pager
```

### 12.6 Cài Kafka tunnel nếu bật Big Data

```bash
sudo APP_ROOT=/opt/mindschool/app bash deploy/install-kafka-tunnel.sh
```

Kiểm tra:

```bash
sudo systemctl status mindschool-kafka-tunnel.service --no-pager
sudo systemctl is-active mindschool-kafka-tunnel.service
```

### 12.7 Deploy Web production

```bash
cd /opt/mindschool/app
sudo APP_ROOT=/opt/mindschool/app bash deploy/deploy.sh
```

Lệnh Docker Compose thủ công:

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  up -d --build --remove-orphans --wait --wait-timeout 300
```

Kiểm tra container:

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  ps
```

### 12.8 Kiểm tra sau deploy

Kiểm tra public:

```bash
curl -I https://mindschool.site
curl -sS https://mindschool.site/ready
```

Kiểm tra local:

```bash
curl -sS http://127.0.0.1:18000/ready
curl -sS http://127.0.0.1:18080/health
curl -sS http://127.0.0.1:18501/dashboard/_stcore/health
```

Xem log:

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  logs --tail=200 backend
```

### 12.9 Deploy Big Data automation

Dry-run:

```bash
bash deploy/gcp/deploy-bigdata-automation.sh --dry-run
```

Deploy thật:

```bash
bash deploy/gcp/deploy-bigdata-automation.sh --disable-host-timer
```

Kiểm tra Scheduler:

```bash
gcloud scheduler jobs list --location=asia-southeast1
```

Kiểm tra Workflow:

```bash
gcloud workflows executions list nightly-dashboard-refresh \
  --location=asia-southeast1 \
  --limit=10
```

Kiểm tra Dataproc batch:

```bash
gcloud dataproc batches list \
  --region=asia-southeast1 \
  --limit=10 \
  --sort-by='~createTime'
```

### 12.10 Warm cache dashboard

```bash
bash deploy/gcp/warm-dashboard-cache.sh
```

## 13. Hướng dẫn sử dụng Web

### 13.1 Truy cập web

Người dùng truy cập:

```text
https://mindschool.site
```

Vai trò chính:

```text
student      học sinh/sinh viên
researcher   người nghiên cứu
```

### 13.2 Đăng ký

Người dùng thực hiện:

1. Nhập email.
2. Nhập mật khẩu.
3. Chọn vai trò.
4. Điền thông tin hồ sơ.
5. Bấm đăng ký.

Thông tin hồ sơ giúp hệ thống phân tích theo nhóm:

```text
học sinh
sinh viên
giới tính
độ tuổi
demographic
```

### 13.3 Đăng nhập

Người dùng nhập email và mật khẩu.

Sau khi đăng nhập:

```text
student      vào trang khảo sát/chatbot
researcher   vào dashboard phân tích
```

### 13.4 Tài khoản học sinh/sinh viên

Chức năng chính:

```text
Làm khảo sát
Chatbot hỗ trợ
Cập nhật hồ sơ
```

#### Làm khảo sát

Quy trình:

```text
Đăng nhập
  -> mở khảo sát
  -> trả lời câu hỏi
  -> gửi khảo sát
  -> hệ thống lưu PostgreSQL
  -> worker ghi GCS
  -> Spark xử lý
  -> dashboard cập nhật sau batch
```

Lưu ý:

- gửi khảo sát xong dashboard có thể chưa đổi ngay
- dashboard chỉ đọc dữ liệu đã xử lý ở Gold
- batch tự động chạy lúc 00:00 hằng ngày

#### Sử dụng chatbot

Quy trình:

```text
Đăng nhập
  -> mở chatbot
  -> nhập câu hỏi
  -> chatbot trả lời
  -> chatlog ghi Kafka
  -> Kafka consumer ghi GCS Silver
  -> Spark xử lý sang Gold
```

Lưu ý:

- chatbot chỉ hỗ trợ thông tin và định hướng tinh thần
- chatbot không thay thế bác sĩ hoặc chuyên gia tâm lý
- chatlog được dùng cho thống kê rủi ro/chủ đề/sentiment

#### Cập nhật hồ sơ

Người dùng có thể cập nhật thông tin cá nhân được phép chỉnh sửa.

Thông tin hồ sơ phục vụ:

- phân nhóm người dùng
- phân tích dashboard
- gắn metadata cho survey/chatlog

### 13.5 Tài khoản nghiên cứu

Tài khoản nghiên cứu dùng để xem dashboard.

Các màn hình chính:

```text
Tổng quan
Học sinh
Sinh viên
Giám sát xử lý dữ liệu
Hồ sơ cá nhân
```

#### Tổng quan

Hiển thị:

- tổng số phản hồi khảo sát
- xu hướng theo thời gian
- phân bố nhóm người học
- chỉ số sức khỏe tinh thần
- thống kê từ chatbot

#### Học sinh

Lọc dữ liệu:

```text
audience_group = school
```

Phân tích:

- áp lực học tập
- áp lực gia đình
- an toàn trường học
- bạn bè
- giấc ngủ
- vận động
- cụm rủi ro

#### Sinh viên

Lọc dữ liệu:

```text
audience_group = university
```

Phân tích:

- áp lực tài chính
- thích nghi học thuật
- cảm giác thuộc về
- phân biệt đối xử
- an toàn khuôn viên
- quan hệ
- chất kích thích
- giấc ngủ

#### Giám sát xử lý dữ liệu

Dùng để xem:

- batch Spark gần nhất
- trạng thái survey/chat
- thời điểm Gold cập nhật
- trạng thái cache dashboard
- thông tin Spark UI/Dataproc logs

Nếu dashboard chưa có dữ liệu mới, kiểm tra:

```text
Silver có dữ liệu chưa
Dataproc batch chạy thành công chưa
Gold table cập nhật chưa
Warm cache chạy chưa
Dashboard đọc cache mới chưa
```

## 14. Cách đọc Dashboard

Dashboard đọc dữ liệu tổng hợp, không đọc raw data.

Survey Gold:

```text
survey_overview_summary
survey_response_by_date
survey_demographic_summary
survey_analytic_features
```

Chat Gold:

```text
chat_hourly_metrics
chat_risk_summary
chat_topic_summary
chat_construct_summary
chat_model_usage
chat_sentiment_summary
```

Ý nghĩa:

- survey phản ánh dữ liệu đã snapshot và chạy Spark
- chat phản ánh dữ liệu đã xuống Silver và chạy Gold
- dashboard có độ trễ do batch/cache
- lịch tự động hiện tại là 00:00 hằng ngày theo giờ Việt Nam
