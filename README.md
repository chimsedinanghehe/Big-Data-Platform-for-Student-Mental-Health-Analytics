# MindSchool - Big Data Platform for Student Mental Health Analytics

README này hướng dẫn triển khai đầy đủ hệ thống MindSchool gồm Web, Backend, PostgreSQL, Dashboard, Kafka/GCS integration và lịch xử lý Big Data bằng Google Cloud Scheduler + Workflows + Dataproc Serverless Spark.

## 1. Kiến trúc triển khai

MindSchool được triển khai theo mô hình hybrid: phần Web/API/Dashboard chạy trên một VPS bằng Docker Compose, còn phần lưu trữ dữ liệu lớn và xử lý batch chạy trên Google Cloud. Cách triển khai này giúp web phản hồi nhanh trên máy chủ cố định, trong khi các job nặng như Spark, GCS, Workflow và Scheduler được tách sang GCP để dễ mở rộng và dễ giám sát.

### 1.1 Tổng quan triển khai

Hệ thống được triển khai theo kiểu tách trách nhiệm. VPS chỉ chạy phần phục vụ người dùng và các worker nhẹ. Google Cloud đảm nhiệm phần lưu trữ data lake và xử lý Spark batch. Kafka VM đứng riêng cho luồng streaming chatlog.

Cell thành phần triển khai:

```text
public_domain        mindschool.site, www.mindschool.site
public_gateway       Nginx trên host, port 80/443
frontend_runtime     Docker container frontend, bind 127.0.0.1:18080
backend_runtime      Docker container backend, bind 127.0.0.1:18000
dashboard_runtime    Docker container dashboard, bind 127.0.0.1:18501
database_runtime     PostgreSQL Docker volume postgres_data
survey_worker        survey-snapshot-worker trong Docker Compose
streaming_runtime    Kafka VM + Kafka consumer service
data_lake            GCS bucket student-mental-health-lake-nhom1-2026
batch_runtime        Cloud Scheduler, Workflows, Dataproc Serverless Spark
```

Cell kiểm tra nhanh sau deploy:

```bash
curl -I https://mindschool.site
curl -sS https://mindschool.site/ready
sudo docker compose --env-file deploy/.env.production -f deploy/docker-compose.nginx-host.yml -f deploy/docker-compose.nginx-host.bigdata.yml ps
gcloud scheduler jobs list --location=asia-southeast1
gcloud dataproc batches list --region=asia-southeast1 --limit=5 --sort-by='~createTime'
```

### 1.2 Các tầng triển khai

Hệ thống được chia thành 5 tầng rõ ràng:

```text
Tầng 1 - Public Gateway
  Nginx host nhận toàn bộ traffic HTTP/HTTPS.

Tầng 2 - Application Runtime
  Docker Compose chạy frontend, backend, dashboard, PostgreSQL và worker.

Tầng 3 - Streaming Ingestion
  Backend publish chatlog sang Kafka, consumer ghi xuống GCS Silver.

Tầng 4 - Data Lake
  GCS lưu Bronze, Silver, Gold, script Spark, vector/embedding artifact.

Tầng 5 - Batch Analytics
  Cloud Scheduler gọi Workflow, Workflow submit Dataproc Serverless Spark.
```

Tách tầng như vậy giúp:

- Nginx là điểm public duy nhất, container không mở trực tiếp ra Internet.
- Backend và dashboard chỉ bind localhost, giảm bề mặt tấn công.
- PostgreSQL chỉ nằm trong Docker network, không expose public port.
- Dữ liệu phân tích được đưa sang GCS/Spark, không ép VPS xử lý batch nặng.
- Scheduler/Workflow nằm trên GCP nên lịch xử lý không phụ thuộc cron của máy chủ.

### 1.3 Public gateway và reverse proxy

Nginx chạy trực tiếp trên host, không chạy trong container. Nginx chịu trách nhiệm:

- nhận traffic từ `mindschool.site` và `www.mindschool.site`
- redirect HTTP sang HTTPS
- phục vụ ACME challenge cho Certbot
- terminate TLS bằng Let's Encrypt
- reverse proxy request tới container nội bộ

Routing production được cấu hình ở Nginx như sau:

```text
route=/                         upstream=frontend_app   local_port=127.0.0.1:18080
route=/api/                     upstream=backend_app    local_port=127.0.0.1:18000
route=/ready                    upstream=backend_app    local_port=127.0.0.1:18000
route=/docs                     upstream=backend_app    local_port=127.0.0.1:18000
route=/openapi.json             upstream=backend_app    local_port=127.0.0.1:18000
route=/dashboard/               upstream=dashboard_app  local_port=127.0.0.1:18501
```

Chỉ các port public sau cần mở:

```text
80/tcp    HTTP và Let's Encrypt challenge
443/tcp   HTTPS production
22/tcp    SSH quản trị
```

Các port ứng dụng chỉ bind vào localhost:

```text
Frontend   127.0.0.1:18080
Backend    127.0.0.1:18000
Dashboard  127.0.0.1:18501
```

### 1.4 Docker Compose runtime

Docker Compose production gồm 2 file chính:

```text
deploy/docker-compose.nginx-host.yml
deploy/docker-compose.nginx-host.bigdata.yml
```

File `docker-compose.nginx-host.yml` chứa phần lõi của web:

```text
postgres
backend
frontend
dashboard
```

File `docker-compose.nginx-host.bigdata.yml` bổ sung phần Big Data:

```text
survey-snapshot-worker
Kafka/GCS readiness cho backend
GCP credential mount cho backend/dashboard/worker
```

Vai trò từng service:

```text
postgres
  Lưu user, session, profile, survey runtime data.
  Dữ liệu nằm trong Docker volume postgres_data.

backend
  FastAPI service.
  Xử lý auth, profile, survey, chatbot, RAG, Kafka publish, GCS readiness.

frontend
  React app đã build static.
  Được Nginx proxy qua 127.0.0.1:18080.

dashboard
  Streamlit dashboard.
  Đọc Gold tables/cache từ GCS.
  Dùng volume dashboard_cache để tăng tốc.

survey-snapshot-worker
  Worker nền.
  Đọc survey từ PostgreSQL.
  Ghi snapshot Parquet lên GCS Bronze.
```

Các service đều có:

- `restart: unless-stopped`
- healthcheck riêng
- log rotation `max-size=10m`, `max-file=5`
- `no-new-privileges:true`
- tmpfs cho `/tmp` để hạn chế rác runtime

### 1.5 Kết nối backend, database và secret

Backend kết nối PostgreSQL qua Docker internal DNS:

```text
postgres:5432
```

Connection string được dựng từ biến môi trường:

```text
DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
```

GCP credential được mount bằng Docker secret:

```text
Host:
  deploy/secrets/gcp-service-account.json

Container:
  /run/secrets/gcp_service_account
```

Các container cần GCP access:

```text
backend
dashboard
survey-snapshot-worker
```

Không đưa secret vào image Docker. Secret chỉ nằm ở host runtime và được mount khi container chạy.

### 1.6 Kiến trúc chatbot và RAG

Luồng hỏi đáp chatbot được chia thành các bước tuần tự. Frontend chỉ gửi câu hỏi và nhận câu trả lời. Backend chịu trách nhiệm rewrite query, truy xuất vector, gọi model sinh câu trả lời, sau đó ghi chatlog sang Kafka.

```text
step_01  user_browser gửi câu hỏi
step_02  frontend gọi Backend API /api/rag/ask
step_03  backend rewrite query nếu cần
step_04  backend truy xuất Qdrant bằng vector search
step_05  backend dựng prompt từ top-k chunk liên quan
step_06  LLM sinh câu trả lời
step_07  backend trả response về frontend
step_08  backend publish chatlog sang Kafka
```

Backend không đọc trực tiếp toàn bộ tài liệu nghiên cứu khi người dùng hỏi. Tài liệu đã được tiền xử lý thành chunk/vector trước đó. Khi có câu hỏi, backend chỉ truy xuất top-k chunk liên quan từ vector store rồi đưa vào prompt trả lời.

Các thành phần liên quan:

```text
Qdrant
  Lưu vector embeddings và metadata chunk.

GCS bronze/research hoặc bronze/knowledge_base
  Lưu tài liệu nguồn.

GCS gold/rag_chunks/
  Lưu chunk đã chuẩn hóa phục vụ embedding/RAG.

Backend RAG
  Điều phối truy xuất và sinh câu trả lời.
```

### 1.7 Kiến trúc streaming chatlog

Chatlog được thiết kế theo hướng streaming để tin nhắn không phải chờ batch Bronze sang Silver.

Cell luồng ghi chatlog:

```text
producer_service      backend
kafka_topic           student-chat-logs
consumer_service      student-chat-kafka-consumer.service trên Kafka VM
output_layer          GCS Silver
output_prefix         silver/anonymized_chat/date=YYYY-MM-DD/hour=HH/
partition_timezone    Asia/Ho_Chi_Minh
```

Đặc điểm chính:

- Backend chỉ publish event vào Kafka, không tự xử lý batch nặng.
- Kafka consumer gom message theo batch nhỏ.
- Consumer ghi trực tiếp Parquet xuống `silver/anonymized_chat`.
- Partition theo `date` và `hour` theo giờ Việt Nam.
- Offset Kafka chỉ commit sau khi upload GCS thành công.
- Message lỗi JSON/schema đi vào DLQ topic.

Output Silver:

```text
gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/date=YYYY-MM-DD/hour=HH/
```

Sau đó Spark batch `chat_silver_to_gold_spark.py` đọc Silver và ghi Gold cho dashboard:

```text
chat_hourly_metrics
chat_risk_summary
chat_topic_summary
chat_construct_summary
chat_model_usage
chat_sentiment_summary
```

### 1.8 Kiến trúc survey pipeline

Survey đi theo mô hình snapshot + batch. Dữ liệu người dùng được lưu trước trong PostgreSQL để ứng dụng phản hồi ngay. Worker nền tạo snapshot lên GCS. Spark batch xử lý snapshot đó thành Silver và Gold cho dashboard.

```text
step_01  frontend gửi survey form
step_02  backend lưu survey vào PostgreSQL
step_03  survey-snapshot-worker đọc dữ liệu survey từ PostgreSQL
step_04  worker ghi snapshot lên bronze/app_survey_snapshot/survey_all.parquet
step_05  Dataproc chạy survey_bronze_to_silver_spark.py
step_06  Spark ghi silver/survey_cleaned/
step_07  Dataproc chạy survey_silver_to_gold_spark.py
step_08  Spark ghi gold/dashboard_tables/
step_09  dashboard đọc Gold hoặc cache từ Gold
```

Lý do survey dùng snapshot + batch:

- Survey có nhiều câu hỏi và nhiều cột phân tích.
- Cần chuẩn hóa schema trước khi đưa vào dashboard.
- Cần tính các chỉ số/cụm theo nhóm học sinh/sinh viên.
- Batch Spark phù hợp hơn cho tổng hợp theo ngày.

Survey Bronze:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/app_survey_snapshot/survey_all.parquet
```

Survey Silver:

```text
gs://student-mental-health-lake-nhom1-2026/silver/survey_cleaned/
```

Survey Gold:

```text
gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_overview_summary/
gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_response_by_date/
gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_demographic_summary/
gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_analytic_features/
```

### 1.9 Kiến trúc batch orchestration trên GCP

Lịch xử lý Big Data không phụ thuộc cron local. Hệ thống dùng Cloud Scheduler gọi Workflow. Workflow submit lần lượt các Dataproc Serverless Spark batch.

```text
scheduler_name       nightly-dashboard-refresh-0000-vn
scheduler_cron       0 0 * * *
scheduler_timezone   Asia/Ho_Chi_Minh
workflow_name        nightly-dashboard-refresh
batch_01             survey_bronze_to_silver
batch_02             survey_silver_to_gold
batch_03             chat_silver_to_gold
```

Workflow tạo batch ID theo ngày xử lý:

```text
ms-YYYYMMDD-HHMMSS-survey-b2s
ms-YYYYMMDD-HHMMSS-survey-s2g
ms-YYYYMMDD-HHMMSS-chat-s2g
```

Lịch hiện tại:

```text
00:00 hằng ngày theo giờ Việt Nam
```

Batch lúc 00:00 xử lý dữ liệu ngày hôm trước theo giờ Việt Nam:

```text
process_date = yesterday(Asia/Ho_Chi_Minh)
```

Spark resource mặc định:

```text
spark.executor.instances = 2
spark.executor.cores = 4
spark.executor.memory = 8g
spark.driver.cores = 4
spark.driver.memory = 8g
spark.default.parallelism = 8
spark.sql.shuffle.partitions = 8
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
driver/executor disk = 250g
```

### 1.10 Kiến trúc dashboard và cache

Dashboard không tính toán lại từ raw data mỗi lần người dùng mở web. Dashboard đọc dữ liệu đã xử lý ở Gold và cache lại để phản hồi nhanh.

```text
data_source       GCS Gold tables
runtime           dashboard container
cache_volume      dashboard_cache
cache_path        /app/data/.dashboard_cache
ui_framework      Streamlit
```

Cache volume:

```text
dashboard_cache:/app/data/.dashboard_cache
```

Sau khi Spark ghi Gold mới, chạy warm cache:

```text
deploy/gcp/warm-dashboard-cache.sh
```

Mục tiêu:

- giảm thời gian mở dashboard
- tránh đọc GCS quá nhiều khi nhiều người xem
- đảm bảo dashboard đọc bản Gold mới sau batch

### 1.11 Phân tách trách nhiệm giữa VPS và GCP

VPS chịu trách nhiệm chạy phần tương tác người dùng:

```text
Nginx
Frontend
Backend
PostgreSQL
Dashboard
survey-snapshot-worker
Kafka SSH tunnel
```

GCP chịu trách nhiệm chạy phần dữ liệu lớn:

```text
GCS Data Lake
Cloud Scheduler
Workflows
Dataproc Serverless Spark
Cloud Logging
Spark UI
```

Kafka VM chịu trách nhiệm streaming trung gian:

```text
Kafka broker
chat consumer service
DLQ topic
GCS Silver writer
```

Cách chia này giúp hệ thống không dồn toàn bộ tải Spark lên VPS dùng chung.

### 1.12 Boundary bảo mật và vận hành

Các boundary quan trọng:

```text
Internet chỉ nhìn thấy Nginx 80/443.
Container app chỉ bind localhost, không public trực tiếp.
PostgreSQL không expose public port.
GCP credential mount bằng Docker secret.
Kafka public broker port không mở; backend đi qua SSH tunnel.
Dashboard đọc Gold/cache, không đọc raw PostgreSQL trực tiếp cho phân tích lớn.
```

Các file/runtime quan trọng:

```text
deploy/.env.production
deploy/secrets/gcp-service-account.json
deploy/secrets/kafka-vm-ssh-key
/etc/nginx/conf.d/mindschool.site.conf
/etc/letsencrypt/live/mindschool.site/
```

Không xóa các file/volume này nếu chưa backup hoặc chưa xác nhận.

### 1.13 Tóm tắt kiến trúc bằng lời

MindSchool dùng Nginx làm cổng HTTPS public. Bên trong VPS, Docker Compose chạy frontend, backend, dashboard, PostgreSQL và survey worker. Backend phục vụ API, chatbot và publish chatlog sang Kafka. Kafka consumer ghi chatlog xuống GCS Silver. Survey worker ghi snapshot survey xuống GCS Bronze. Cloud Scheduler gọi Workflow lúc 00:00 hằng ngày để submit các batch Spark trên Dataproc, sinh Silver/Gold cho dashboard. Dashboard đọc Gold và cache lại để tăng tốc hiển thị.

Cell kiểm chứng kiến trúc:

```bash
sudo nginx -t
curl -sS https://mindschool.site/ready
sudo docker compose --env-file deploy/.env.production -f deploy/docker-compose.nginx-host.yml -f deploy/docker-compose.nginx-host.bigdata.yml ps
gcloud scheduler jobs describe nightly-dashboard-refresh-0000-vn --location=asia-southeast1
gcloud workflows describe nightly-dashboard-refresh --location=asia-southeast1
gcloud dataproc batches list --region=asia-southeast1 --limit=10 --sort-by='~createTime'
```

## 2. Luồng dữ liệu

### 2.1 Web và ứng dụng

```text
step_01  frontend gửi request
step_02  backend xử lý nghiệp vụ
step_03  PostgreSQL lưu metadata ứng dụng
```

Backend xử lý:

- đăng ký, đăng nhập, phân quyền
- survey học sinh/sinh viên
- chatbot/RAG
- ghi log chat sang Kafka
- ghi snapshot survey để Big Data pipeline đọc

### 2.2 Chatlogs

Luồng chat hiện tại:

```text
step_01  backend publish event vào Kafka topic student-chat-logs
step_02  Kafka consumer trên VM đọc message
step_03  consumer ghi Parquet xuống silver/anonymized_chat/date=YYYY-MM-DD/hour=HH/
step_04  Dataproc chạy chat_silver_to_gold_spark.py
step_05  Spark ghi các bảng Gold phục vụ dashboard
```

Chatlogs hiện đi thẳng từ Kafka xuống Silver. Không còn bắt buộc ghi Bronze theo lô cho chatlogs.

Các Gold output chính của chat:

```text
gold/dashboard_tables/chat_hourly_metrics/
gold/dashboard_tables/chat_risk_summary/
gold/dashboard_tables/chat_topic_summary/
gold/dashboard_tables/chat_construct_summary/
gold/dashboard_tables/chat_model_usage/
gold/sentiment_summary/chat_sentiment_summary/
```

### 2.3 Survey

Luồng survey:

```text
Backend/PostgreSQL
step_01  survey-snapshot-worker đọc survey từ PostgreSQL
step_02  worker ghi bronze/app_survey_snapshot/survey_all.parquet
step_03  Dataproc chạy survey_bronze_to_silver_spark.py
step_04  Spark ghi silver/survey_cleaned/
step_05  Dataproc chạy survey_silver_to_gold_spark.py
step_06  Spark ghi gold/dashboard_tables/
```

Các Gold output chính của survey:

```text
gold/dashboard_tables/survey_overview_summary/
gold/dashboard_tables/survey_response_by_date/
gold/dashboard_tables/survey_demographic_summary/
gold/dashboard_tables/survey_analytic_features/
```

### 2.4 RAG nghiên cứu

Luồng tài liệu nghiên cứu:

```text
GCS bronze/research hoặc bronze/knowledge_base
step_01  đọc tài liệu nguồn từ GCS
step_02  làm sạch và chia chunk
step_03  tạo vector embedding
step_04  lưu vector vào Qdrant
step_05  Backend RAG truy xuất khi người dùng hỏi
```

## Hướng dẫn sử dụng Web

Sau khi triển khai thành công, người dùng truy cập:

```text
https://mindschool.site
```

Nếu dùng môi trường local hoặc staging, thay domain bằng địa chỉ frontend tương ứng.

### 1. Đăng ký tài khoản

Tại màn hình đăng ký:

1. Nhập email.
2. Nhập mật khẩu.
3. Chọn vai trò tài khoản.
4. Điền thông tin hồ sơ theo vai trò.
5. Bấm đăng ký.

Các vai trò chính:

```text
student      học sinh/sinh viên dùng survey và chatbot
researcher   người nghiên cứu xem dashboard, giám sát dữ liệu và phân tích
```

Với tài khoản học sinh/sinh viên, hệ thống cần các thông tin hồ sơ cơ bản như nhóm người học, giới tính, ngày sinh hoặc thông tin phân loại tương ứng. Các thông tin này giúp hệ thống phân tách dữ liệu theo nhóm học sinh/sinh viên trong dashboard và trong pipeline Big Data.

Nếu đăng ký không thành công, kiểm tra:

- email đã tồn tại chưa
- mật khẩu có hợp lệ không
- đã điền đủ trường bắt buộc chưa
- backend `/ready` có đang OK không

### 2. Đăng nhập

Tại màn hình đăng nhập:

1. Nhập email.
2. Nhập mật khẩu.
3. Bấm đăng nhập.

Sau khi đăng nhập, frontend tự điều hướng theo vai trò:

```text
student      vào giao diện học sinh/sinh viên
researcher   vào giao diện nghiên cứu/dashboard
```

Token đăng nhập được frontend lưu ở trình duyệt. Khi đăng xuất, token bị xóa khỏi trình duyệt.

### 3. Sử dụng tài khoản học sinh/sinh viên

Tài khoản học sinh/sinh viên dùng các chức năng chính:

```text
Khảo sát sức khỏe tinh thần
Chatbot hỗ trợ
Hồ sơ cá nhân
```

#### 3.1 Làm khảo sát

Quy trình làm khảo sát:

1. Đăng nhập bằng tài khoản học sinh/sinh viên.
2. Mở mục khảo sát.
3. Trả lời lần lượt các câu hỏi.
4. Bấm gửi khi hoàn thành.
5. Chờ hệ thống báo gửi thành công.

Sau khi gửi:

```text
Survey response -> PostgreSQL -> survey-snapshot-worker -> GCS Bronze snapshot
```

Worker định kỳ gom dữ liệu survey từ PostgreSQL và cập nhật:

```text
gs://student-mental-health-lake-nhom1-2026/bronze/app_survey_snapshot/survey_all.parquet
```

Đến lịch batch hằng ngày, Spark xử lý:

```text
bronze/app_survey_snapshot/
  -> silver/survey_cleaned/
  -> gold/dashboard_tables/
```

Dashboard không đọc trực tiếp câu trả lời thô của người dùng từ web. Dashboard đọc dữ liệu đã được xử lý ở Gold hoặc cache từ Gold.

#### 3.2 Sử dụng chatbot

Quy trình chat:

1. Đăng nhập bằng tài khoản học sinh/sinh viên.
2. Mở mục chatbot.
3. Nhập nội dung cần hỏi.
4. Bấm gửi hoặc nhấn Enter.
5. Đợi chatbot phản hồi.

Khi người dùng nhắn tin:

```text
Frontend
  -> Backend RAG/chat API
  -> chatbot sinh câu trả lời
  -> backend publish chatlog vào Kafka
  -> Kafka consumer ghi xuống GCS Silver
```

Chatlog được ẩn danh trước khi đưa vào pipeline phân tích. Các thông tin như email hoặc số điện thoại trong nội dung chat được mask ở tầng xử lý log.

Chatbot dùng để hỗ trợ thông tin và định hướng tinh thần, không thay thế chuyên gia y tế/tâm lý. Với nội dung có nguy cơ cao, hệ thống phân loại vào nhóm rủi ro để dashboard có thể thống kê và giám sát.

#### 3.3 Cập nhật hồ sơ cá nhân

Người dùng có thể mở mục hồ sơ để xem hoặc cập nhật thông tin cá nhân được phép chỉnh sửa. Các thông tin hồ sơ phục vụ:

- phân nhóm học sinh/sinh viên
- phân tích dashboard theo demographic
- gắn metadata cho chatlog/survey

### 4. Sử dụng tài khoản nghiên cứu

Tài khoản nghiên cứu dùng để xem dashboard và theo dõi hệ thống phân tích.

Các màn hình chính:

```text
Tổng quan
Học sinh
Sinh viên
Giám sát xử lý dữ liệu
Hồ sơ cá nhân
```

#### 4.1 Tổng quan

Màn hình tổng quan dùng để xem bức tranh chung:

- tổng số phản hồi khảo sát
- xu hướng theo thời gian
- phân bố nhóm người học
- các chỉ số sức khỏe tinh thần tổng hợp
- tín hiệu từ chatbot như rủi ro, chủ đề, cảm xúc/sentiment nếu có

Dữ liệu tổng quan được đọc từ các bảng Gold đã xử lý bởi Spark.

#### 4.2 Học sinh

Màn hình học sinh lọc dữ liệu nhóm:

```text
audience_group = school
```

Các phân tích thường gồm:

- áp lực học tập
- áp lực gia đình
- an toàn trường học/bạn bè
- giấc ngủ, vận động, phục hồi
- các cụm rủi ro liên quan đến học sinh

#### 4.3 Sinh viên

Màn hình sinh viên lọc dữ liệu nhóm:

```text
audience_group = university
```

Các phân tích thường gồm:

- áp lực tài chính
- thích nghi học thuật
- cảm giác thuộc về
- phân biệt đối xử
- an toàn khuôn viên
- quan hệ, sang chấn, chất kích thích
- giấc ngủ và phục hồi

#### 4.4 Giám sát xử lý dữ liệu

Màn hình giám sát xử lý dữ liệu dùng để kiểm tra trạng thái Big Data pipeline:

- batch Spark gần nhất
- trạng thái xử lý survey/chat
- thời điểm Gold được cập nhật
- trạng thái cache dashboard
- link hoặc thông tin hỗ trợ mở Spark UI/Dataproc logs

Nếu dashboard chưa thấy dữ liệu mới, kiểm tra theo thứ tự:

```text
GCS Silver có dữ liệu mới chưa
Dataproc batch có chạy thành công chưa
Gold table có cập nhật chưa
Warm cache dashboard đã chạy chưa
Dashboard container có đọc cache mới chưa
```

### 5. Cách đọc số liệu trên dashboard

Dashboard đọc dữ liệu đã tổng hợp, không đọc raw event trực tiếp. Các bảng chính:

```text
Survey Gold:
  survey_overview_summary
  survey_response_by_date
  survey_demographic_summary
  survey_analytic_features

Chat Gold:
  chat_hourly_metrics
  chat_risk_summary
  chat_topic_summary
  chat_construct_summary
  chat_model_usage
  chat_sentiment_summary
```

Khi xem biểu đồ:

- số liệu survey phản ánh dữ liệu đã được snapshot và chạy Spark batch
- số liệu chat phản ánh chatlog đã xuống Silver và được chạy Chat Silver -> Gold
- các chart có thể chưa đổi ngay sau khi người dùng vừa gửi survey/chat vì dashboard đọc Gold/cache, không đọc trực tiếp từ form

### 6. Độ trễ dữ liệu người dùng cần hiểu

Luồng realtime và batch khác nhau:

```text
Chatlog:
  thường xuống Silver nhanh qua Kafka consumer
  Gold/dashboard cập nhật sau batch Chat Silver -> Gold hoặc sau khi chạy thủ công

Survey:
  lưu vào PostgreSQL trước
  snapshot-worker cập nhật Bronze snapshot
  Gold/dashboard cập nhật sau batch Survey Bronze -> Silver -> Gold
```

Lịch tự động hiện tại:

```text
00:00 hằng ngày theo giờ Việt Nam
```

Batch lúc 00:00 xử lý dữ liệu ngày hôm trước theo giờ Việt Nam.

### 7. Lỗi thường gặp khi sử dụng Web

#### Không đăng nhập được

Kiểm tra:

- email/mật khẩu đúng chưa
- tài khoản đã đăng ký chưa
- backend có đang chạy không
- database PostgreSQL có healthy không

Lệnh kiểm tra backend:

```bash
curl -sS https://mindschool.site/ready
```

#### Chatbot báo Failed to Fetch

Nguyên nhân thường gặp:

- backend container lỗi
- Nginx proxy lỗi
- CORS/API base URL sai
- OpenAI/RAG/Qdrant lỗi
- Kafka readiness lỗi nếu đang bật `READINESS_REQUIRE_KAFKA=true`

Kiểm tra:

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  logs --tail=200 backend
```

#### Gửi khảo sát xong nhưng dashboard chưa đổi

Đây có thể là hành vi bình thường nếu batch chưa chạy. Kiểm tra:

```bash
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/bronze/app_survey_snapshot/
gcloud dataproc batches list --region=asia-southeast1 --limit=10 --sort-by='~createTime'
```

Nếu batch đã chạy thành công nhưng dashboard vẫn chưa đổi, warm cache:

```bash
bash deploy/gcp/warm-dashboard-cache.sh
```

#### Chat xong nhưng dashboard chưa đổi

Kiểm tra chat đã xuống Silver chưa:

```bash
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/
```

Sau đó kiểm tra batch Chat Silver -> Gold:

```bash
gcloud dataproc batches list --region=asia-southeast1 --limit=10 --sort-by='~createTime'
```

Nếu cần cập nhật ngay dashboard, chạy workflow hoặc batch Chat Silver -> Gold thủ công theo phần vận hành bên dưới.

## 3. Yêu cầu máy chủ

Máy chủ cần có:

```text
Ubuntu Server
Docker + Docker Compose v2
Nginx
Certbot
Git
Google Cloud CLI nếu triển khai Big Data automation từ máy này
```

Port cần mở:

```text
80/tcp    HTTP, Certbot challenge
443/tcp   HTTPS
22/tcp    SSH quản trị
```

Docker service nội bộ chỉ bind localhost:

```text
backend    127.0.0.1:18000
frontend   127.0.0.1:18080
dashboard  127.0.0.1:18501
```

Nginx là tầng public duy nhất nhận traffic web.

## 4. Chuẩn bị DNS

Trước khi cấp SSL, DNS phải trỏ về IP máy deploy.

Ví dụ Cloudflare:

```text
mindschool.site       A     <SERVER_PUBLIC_IP>
www.mindschool.site   A     <SERVER_PUBLIC_IP>
*.mindschool.site     A     <SERVER_PUBLIC_IP>
```

Nếu dùng Cloudflare proxy, cần đảm bảo chế độ SSL phù hợp. Khi cấp Let's Encrypt trực tiếp trên máy, HTTP port 80 phải truy cập được từ Internet.

## 5. Chuẩn bị mã nguồn

Clone project vào thư mục triển khai:

```bash
sudo mkdir -p /opt/mindschool
sudo chown "$USER:$USER" /opt/mindschool
git clone <REPOSITORY_URL> /opt/mindschool/app
cd /opt/mindschool/app
```

Nếu đang deploy trực tiếp tại workspace hiện tại:

```bash
cd /home/tranmanhcuong/Big-Data-Platform-for-Student-Mental-Health-Analytics
```

## 6. Cấu hình môi trường

Tạo file production env từ mẫu:

```bash
cp deploy/.env.production.example deploy/.env.production
```

Sửa các biến quan trọng:

```text
APP_DOMAIN=mindschool.site
ACME_EMAIL=<email nhận thông báo SSL>

POSTGRES_DB=student_mental_health_app
POSTGRES_USER=student_app
POSTGRES_PASSWORD=<mật khẩu mạnh>

OPENAI_API_KEY=<OpenAI API key>
OPENAI_MODEL=<model dùng cho chatbot>

QDRANT_URL=<Qdrant URL>
QDRANT_API_KEY=<Qdrant API key nếu có>
QDRANT_COLLECTION=<collection đang dùng>

GCP_PROJECT_ID=student-mental-health-496205
GCP_REGION=asia-southeast1
GCS_BUCKET_NAME=student-mental-health-lake-nhom1-2026
GCS_BUCKET=student-mental-health-lake-nhom1-2026

BIGDATA_ENABLED=true
READINESS_REQUIRE_GCS=true
READINESS_REQUIRE_KAFKA=true

KAFKA_SSH_HOST=<Kafka VM public IP>
KAFKA_SSH_USER=Admin
KAFKA_SSH_PORT=22
KAFKA_LOCAL_PORT=9092
KAFKA_REMOTE_HOST=127.0.0.1
KAFKA_REMOTE_PORT=9092

CHAT_KAFKA_ENABLED=true
CHAT_KAFKA_TOPIC=student-chat-logs
```

Không commit file `deploy/.env.production` lên Git.

## 7. Chuẩn bị secret GCP

Đặt file credential tại:

```text
deploy/secrets/gcp-service-account.json
```

File này có thể là:

- service account JSON
- authorized user ADC JSON nếu môi trường đang dùng user credential

Phân quyền file:

```bash
chmod 600 deploy/secrets/gcp-service-account.json
```

Backend, dashboard và worker sẽ mount file này vào container qua Docker secret.

## 8. Cài Nginx, Docker, Certbot

Script cài đặt:

```bash
sudo APP_ROOT=/opt/mindschool/app \
  APP_DOMAIN=mindschool.site \
  ACME_EMAIL=<email> \
  bash deploy/install-nginx-certbot.sh
```

Script sẽ:

- cài Docker/Nginx/Certbot
- mở port 80/443 nếu có UFW
- copy Nginx HTTP config
- kiểm tra ACME challenge
- cấp chứng chỉ Let's Encrypt
- chuyển sang HTTPS config
- bật timer renew certbot

Kiểm tra Nginx:

```bash
sudo nginx -t
sudo systemctl status nginx --no-pager
```

File Nginx production nằm tại:

```text
deploy/nginx/conf.d/mindschool.site.conf
```

Khi deploy, file này được copy sang:

```text
/etc/nginx/conf.d/mindschool.site.conf
```

## 9. Cài Kafka SSH tunnel nếu bật Big Data

Kafka broker không mở public port. Backend kết nối Kafka qua SSH tunnel local.

Kiểm tra biến Kafka trong `deploy/.env.production`:

```text
KAFKA_SSH_HOST
KAFKA_SSH_USER
KAFKA_SSH_PORT
KAFKA_LOCAL_PORT
KAFKA_REMOTE_HOST
KAFKA_REMOTE_PORT
```

Đặt SSH key Kafka VM tại:

```text
deploy/secrets/kafka-vm-ssh-key
```

Phân quyền:

```bash
chmod 600 deploy/secrets/kafka-vm-ssh-key
```

Cài tunnel service:

```bash
sudo APP_ROOT=/opt/mindschool/app bash deploy/install-kafka-tunnel.sh
```

Kiểm tra:

```bash
sudo systemctl status mindschool-kafka-tunnel.service --no-pager
sudo systemctl is-active mindschool-kafka-tunnel.service
```

## 10. Deploy Web production

Deploy đầy đủ bằng script:

```bash
sudo APP_ROOT=/opt/mindschool/app bash deploy/deploy.sh
```

Nếu chạy ngay trong workspace hiện tại:

```bash
sudo APP_ROOT=/home/tranmanhcuong/Big-Data-Platform-for-Student-Mental-Health-Analytics \
  bash deploy/deploy.sh
```

Script sẽ:

- chạy preflight
- kiểm tra Big Data nếu `BIGDATA_ENABLED=true`
- chọn compose file phù hợp
- copy Nginx config
- reload Nginx
- backup PostgreSQL nếu container cũ đang chạy
- build và start Docker services
- chờ healthcheck

Compose chính:

```text
deploy/docker-compose.nginx-host.yml
```

Compose bổ sung Big Data:

```text
deploy/docker-compose.nginx-host.bigdata.yml
```

Lệnh Docker compose thủ công:

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

## 11. Kiểm tra sau deploy

Kiểm tra public endpoint:

```bash
curl -I https://mindschool.site
curl -sS https://mindschool.site/ready
```

Kết quả `/ready` cần có:

```text
database ok
gcs ok nếu READINESS_REQUIRE_GCS=true
kafka ok nếu READINESS_REQUIRE_KAFKA=true
```

Kiểm tra nội bộ:

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
  logs -f backend
```

Các service thường xem:

```text
backend
frontend
dashboard
postgres
survey-snapshot-worker
```

## 12. Deploy Big Data automation trên GCP

Big Data automation dùng:

```text
Cloud Scheduler
  -> Workflows
  -> Dataproc Serverless Spark batches
  -> GCS Silver/Gold
```

Script triển khai:

```bash
bash deploy/gcp/deploy-bigdata-automation.sh --dry-run
bash deploy/gcp/deploy-bigdata-automation.sh --disable-host-timer
```

Script sẽ:

- upload Spark scripts lên GCS
- deploy workflow `nightly-dashboard-refresh`
- tạo hoặc update scheduler `nightly-dashboard-refresh-0000-vn`
- tùy chọn tắt timer local cũ nếu chuyển sang GCP Scheduler

Workflow hiện chạy lúc:

```text
00:00 hằng ngày
Timezone: Asia/Ho_Chi_Minh
```

Workflow xử lý dữ liệu ngày hôm trước theo logic:

```text
process_date = ngày hôm qua theo giờ Việt Nam
```

Các batch tạo ra:

```text
ms-YYYYMMDD-HHMMSS-survey-b2s
ms-YYYYMMDD-HHMMSS-survey-s2g
ms-YYYYMMDD-HHMMSS-chat-s2g
```

Kiểm tra Scheduler:

```bash
gcloud scheduler jobs list --location=asia-southeast1
gcloud scheduler jobs describe nightly-dashboard-refresh-0000-vn --location=asia-southeast1
```

Kiểm tra Workflow:

```bash
gcloud workflows list --location=asia-southeast1
gcloud workflows describe nightly-dashboard-refresh --location=asia-southeast1
gcloud workflows executions list nightly-dashboard-refresh --location=asia-southeast1 --limit=10
```

Chạy thử workflow thủ công:

```bash
gcloud workflows run nightly-dashboard-refresh \
  --location=asia-southeast1 \
  --data='{"project_id":"student-mental-health-496205","region":"asia-southeast1","bucket":"student-mental-health-lake-nhom1-2026"}'
```

## 13. Kiểm tra Dataproc/Spark

Liệt kê batch mới nhất:

```bash
gcloud dataproc batches list \
  --region=asia-southeast1 \
  --limit=12 \
  --sort-by='~createTime'
```

Xem chi tiết một batch:

```bash
gcloud dataproc batches describe <BATCH_ID> --region=asia-southeast1
```

Lấy link Spark UI:

```bash
deploy/gcp/spark-ui-links.sh --latest
deploy/gcp/spark-ui-links.sh --run-id ms-YYYYMMDD-HHMMSS
deploy/gcp/spark-ui-links.sh --batch-id <BATCH_ID>
```

Xem log Spark trong Cloud Logging:

```bash
gcloud logging read \
  'resource.type="cloud_dataproc_batch" AND resource.labels.location="asia-southeast1" AND textPayload:"JOB_JSON_LOG"' \
  --project=student-mental-health-496205 \
  --limit=20 \
  --freshness=48h
```

## 14. Warm cache dashboard

Sau khi Gold mới được sinh ra, dashboard nên warm cache để người dùng mở nhanh hơn.

Chạy thủ công:

```bash
bash deploy/gcp/warm-dashboard-cache.sh
```

Nếu cần dry-run:

```bash
bash deploy/gcp/warm-dashboard-cache.sh --dry-run
```

Kiểm tra timer warm cache nếu có cài systemd:

```bash
systemctl list-timers | grep mindschool
systemctl status mindschool-dashboard-cache-warm.timer --no-pager
```

## 15. Kiểm tra GCS output

Kiểm tra survey Bronze/Silver/Gold:

```bash
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/bronze/app_survey_snapshot/
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/silver/survey_cleaned/
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_overview_summary/
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/survey_analytic_features/
```

Kiểm tra chat Silver/Gold:

```bash
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_hourly_metrics/
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/gold/dashboard_tables/chat_risk_summary/
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/gold/sentiment_summary/chat_sentiment_summary/
```

Với chat, partition phải theo ngày/giờ Việt Nam:

```text
silver/anonymized_chat/date=YYYY-MM-DD/hour=HH/
```

## 16. Cơ chế chịu lỗi

### 16.1 Kafka

Kafka consumer dùng cơ chế at-least-once:

- `enable.auto.commit=false`
- chỉ commit offset sau khi upload GCS thành công
- upload GCS có retry
- malformed message đi vào DLQ topic
- service có `Restart=always`

Các biến quan trọng:

```text
CHAT_KAFKA_DLQ_TOPIC=student-chat-logs-dlq
CHAT_KAFKA_GCS_UPLOAD_RETRIES=3
CHAT_KAFKA_GCS_UPLOAD_RETRY_BACKOFF_SECONDS=2
```

Nếu consumer chết trước khi commit offset, Kafka có thể phát lại message. Tầng Silver/Gold dùng `event_id` để giảm duplicate.

### 16.2 Spark/Dataproc

Spark chịu lỗi ở tầng batch:

- Dataproc Serverless quản lý driver/executor
- Spark retry task nếu task lỗi tạm thời
- Workflow fail-fast nếu batch trả lỗi
- Scheduler retry khi gọi workflow lỗi
- các job có log `JOB_JSON_LOG`
- chat Gold có `dropDuplicates(["event_id"])`
- survey Gold ghi theo `run_id` cho nhiều bảng để dễ audit

Spark batch không dùng DLQ giống Kafka. Với batch file trên GCS, lỗi được xử lý bằng retry, validation, dedup và log audit.

## 17. Dừng và bật lại hệ thống

Dừng Docker app:

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  stop
```

Bật lại:

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  up -d
```

Dừng Kafka tunnel:

```bash
sudo systemctl stop mindschool-kafka-tunnel.service
```

Bật Kafka tunnel:

```bash
sudo systemctl start mindschool-kafka-tunnel.service
```

Pause GCP Scheduler:

```bash
gcloud scheduler jobs pause nightly-dashboard-refresh-0000-vn --location=asia-southeast1
```

Resume GCP Scheduler:

```bash
gcloud scheduler jobs resume nightly-dashboard-refresh-0000-vn --location=asia-southeast1
```

## 18. Cập nhật code và deploy lại

Quy trình cập nhật:

```bash
cd /opt/mindschool/app
git pull
sudo APP_ROOT=/opt/mindschool/app bash deploy/deploy.sh
```

Nếu chỉ cần rebuild một service:

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  up -d --build backend
```

Tương tự:

```text
frontend
dashboard
survey-snapshot-worker
```

## 19. Backup PostgreSQL

Backup thủ công:

```bash
bash deploy/backup-postgres.sh
```

Script deploy cũng backup trước khi recreate service nếu PostgreSQL đang chạy.

Docker volume PostgreSQL:

```text
postgres_data
```

Không xóa volume này nếu chưa backup.

## 20. Troubleshooting nhanh

### Web không vào được

```bash
sudo nginx -t
sudo systemctl status nginx --no-pager
curl -I http://127.0.0.1:18080/health
curl -I https://mindschool.site
```

### Backend lỗi

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  logs --tail=200 backend
```

### Dashboard không đọc dữ liệu mới

```bash
bash deploy/gcp/warm-dashboard-cache.sh
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  restart dashboard
```

### Survey không xuống GCS

Kiểm tra worker:

```bash
sudo docker compose \
  --env-file deploy/.env.production \
  -f deploy/docker-compose.nginx-host.yml \
  -f deploy/docker-compose.nginx-host.bigdata.yml \
  logs --tail=200 survey-snapshot-worker
```

Kiểm tra object:

```bash
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/bronze/app_survey_snapshot/
```

### Chat không xuống Silver

Kiểm tra Kafka tunnel:

```bash
sudo systemctl status mindschool-kafka-tunnel.service --no-pager
```

Kiểm tra Kafka consumer trên VM:

```bash
ssh -i deploy/secrets/kafka-vm-ssh-key Admin@<KAFKA_VM_IP> \
  'systemctl status student-chat-kafka-consumer.service --no-pager -n 50'
```

Kiểm tra Silver:

```bash
gcloud storage ls gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/
```

### Scheduler không chạy

```bash
gcloud scheduler jobs describe nightly-dashboard-refresh-0000-vn --location=asia-southeast1
gcloud workflows executions list nightly-dashboard-refresh --location=asia-southeast1 --limit=10
gcloud dataproc batches list --region=asia-southeast1 --limit=10 --sort-by='~createTime'
```

## 21. Nguyên tắc vận hành trên máy dùng chung

Máy deploy là máy dùng chung, vì vậy:

- không chạy `docker system prune -a` nếu chưa được xác nhận
- không xóa Docker volume PostgreSQL
- không xóa `/etc/nginx/conf.d/*` của project khác
- không xóa certbot certificate của domain khác
- không kill process lạ không thuộc project
- khi dừng hệ thống, chỉ dừng compose/service của MindSchool

Các file cần cẩn thận:

```text
deploy/.env.production
deploy/secrets/gcp-service-account.json
deploy/secrets/kafka-vm-ssh-key
/etc/nginx/conf.d/mindschool.site.conf
/etc/letsencrypt/live/mindschool.site/
```
