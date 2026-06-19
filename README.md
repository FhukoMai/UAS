# Pub-Sub Log Aggregator Terdistribusi

Implementasi dari tugas Ujian Akhir Semester (UAS) Sistem Terdistribusi. Merupakan sebuah multi-service log aggregator berbasis *Publish-Subscribe* yang dibangun menggunakan Python (FastAPI), Redis, dan PostgreSQL, serta dijalankan di atas Docker Compose.

## Arsitektur Layanan
- **Aggregator**: Layanan utama berbasis FastAPI yang menyediakan API `/publish` untuk menerima event, `/events` untuk membaca event, dan `/stats` untuk melihat metrik. Aggregator juga memiliki *internal consumer worker* (berjalan sebagai *background task*) yang mengambil event dari Redis queue dan menyimpannya secara atomik ke PostgreSQL untuk memastikan deduplikasi dan idempotency.
- **Publisher**: Sebuah layanan Python sederhana yang secara berkala mensimulasikan pengiriman event (termasuk duplikasi) ke endpoint `POST /publish` milik Aggregator.
- **Broker (Redis)**: Bertindak sebagai perantara komunikasi asinkron antara penerima API (producer) dan *internal consumer worker* di aggregator, memastikan decouple dan buffer pada saat *traffic spike*.
- **Storage (PostgreSQL)**: Bertindak sebagai media penyimpanan persisten dengan mekanisme proteksi *unique constraints* untuk mencegah race condition.

## Fitur Utama
1. **Idempotent Consumer & Deduplication**: Event dengan pasangan `topic` dan `event_id` yang sama hanya akan diproses dan disimpan satu kali (Idempotent). Dicegah pada level basis data menggunakan constraint `UNIQUE(topic, event_id)` dan klausa `INSERT ... ON CONFLICT DO NOTHING`.
2. **Transaksi dan Konkurensi**: Pada saat *insert* event, aggregator menggunakan koneksi database terpisah (*session per batch/event*) yang memiliki Isolation Level `READ COMMITTED` untuk mencegah *race conditions*. Statistik `received`, `unique_processed`, dan `duplicate_dropped` di-*update* menggunakan operasi SQL atomik.
3. **Persistensi Data**: Menggunakan *named volumes* (`pg_data`, `broker_data`, `aggregator_data`) pada Docker Compose untuk menjamin data (event maupun status deduplikasi) tetap aman meskipun *container* dihapus atau di-*recreate*.
4. **Validasi Skema Event**: Semua *payload* masuk akan divalidasi oleh `Pydantic` pada FastAPI untuk memenuhi skema JSON standar.

## Endpoints
1. `POST /publish`: Endpoint untuk mengirimkan list of events (batch atau single).
   - Skema payload: `{"events": [{"topic": "...", "event_id": "...", "timestamp": "...", "source": "...", "payload": {...}}]}`
2. `GET /events?topic=<topic>`: Melihat 100 event terakhir yang sukses diproses pada suatu topik.
3. `GET /stats`: Melihat statistik sistem seperti jumlah event diterima, jumlah sukses, jumlah duplicate (di-drop), list topik, dan uptime.
4. `GET /health`: Health check status.

## Cara Menjalankan
1. Pastikan Docker dan Docker Compose telah ter-install di sistem.
2. Lakukan clone repositori dan arahkan terminal ke root directory proyek.
3. Jalankan perintah berikut:
   ```bash
   docker compose up --build -d
   ```
4. Sistem akan melakukan build terhadap *image* Python (Aggregator dan Publisher) lalu menjalankannya beserta Redis dan PostgreSQL.
5. Anda dapat mengakses layanan Aggregator di: `http://localhost:8080`.
6. Publisher akan secara otomatis berjalan di *background* untuk mensimulasikan trafik. Bisa dicek lognya dengan `docker compose logs -f`.

## Cara Menjalankan Tests
Pastikan stack Docker Compose sedang berjalan. Test akan mengakses endpoint pada `localhost:8080`.

1. Install requirement test:
   ```bash
   pip install -r tests/requirements.txt
   ```
2. Jalankan pytest di *root directory*:
   ```bash
   pytest tests/
   ```