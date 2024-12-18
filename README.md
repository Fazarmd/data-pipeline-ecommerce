# Final Project Data Pipeline E-Commerce

Proyek ini adalah implementasi data pipeline untuk menganalisis data e-commerce menggunakan teknologi seperti **Apache Spark**, **PostgreSQL**, **Airflow**, dan **Metabase**. Tujuan utama proyek ini adalah untuk mengelola data dari sumber (file CSV), memprosesnya, dan menyediakan analisis berbasis visualisasi.

---
- link PPT : https://docs.google.com/presentation/d/1YI_HRv7jeoSm8CZ_W3aVCdx3oq407-g-5BR79TkR-vA/edit?usp=sharing
---

## **Tujuan**
1. Membuat pipeline ETL (**Extract, Transform, Load**) untuk data e-commerce.
2. Menyimpan data yang terproses ke dalam database PostgreSQL dengan desain **Star Schema**.
3. Menyediakan visualisasi data menggunakan Metabase untuk memberikan insight bisnis.

---

## **Teknologi yang Digunakan**
- **Apache Spark**: Untuk proses ETL (Extract, Transform, Load) data.
- **PostgreSQL**: Untuk penyimpanan data terstruktur dalam database.
- **Apache Airflow**: Untuk mengorkestrasi pipeline ETL.
- **Metabase**: Untuk visualisasi dan analisis data.
- **Docker**: Untuk pengelolaan lingkungan aplikasi.

---

## **Pendekatan Proses Batch**
Proyek ini menggunakan pendekatan **batch processing**, di mana data diproses secara berkala dalam jumlah besar. Batch baru dapat ditambahkan sesuai kebutuhan (misalnya, data baru dari file CSV).

**Langkah-Langkah Proses Batch**:
1. **Extract**: Membaca data dari file CSV.
2. **Transform**: Memproses dan membersihkan data agar sesuai dengan Star Schema.
3. **Load**: Memasukkan data hasil transformasi ke dalam database PostgreSQL.
4. **Visualisasi**: Menganalisis dan memvisualisasikan data menggunakan Metabase.

---

## **Langkah-Langkah Menjalankan Proyek**

### 1. Clone Repository
Clone repository ke komputer lokal Anda:
```bash
git clone <repository-url>
cd <repository-folder>
```

### 2. Jalankan PostgreSQL
Jalankan PostgreSQL sebagai database utama untuk menyimpan data e-commerce:
```bash
make postgres
```
**Fungsi**:
1. Menyediakan database relasional untuk menyimpan data hasil transformasi.
2. Database akan memiliki schema data_pipeline dengan tabel berbasis Star Schema.

### 3. Jalankan Apache Spark
Jalankan Apache Spark untuk proses ETL:
```bash
make spark
```
**Fungsi**:
1. Memproses file CSV dari sumber data menjadi data yang terstruktur.
2. Menyimpan hasil transformasi ke database PostgreSQL.

### 4. Jalankan Apache Airflow
Jalankan Apache Airflow untuk mengorkestrasi pipeline ETL:
```bash
make airflow
```
**Fungsi**:
1. Menjadwalkan dan mengelola pipeline ETL secara otomatis.
2. Menjalankan proses Extract, Transform, dan Load secara berurutan.

### 5. Jalankan Metabase
Jalankan Metabase untuk visualisasi data:
```bash
make metabase
```
**Fungsi**:
1. Menghubungkan ke database PostgreSQL untuk menampilkan data dalam bentuk dashboard.
2. Memberikan insight bisnis melalui visualisasi data seperti pie chart, line chart, dan lainnya.

---

## **Pipeline ETL**

Pipeline ETL terdiri dari tiga langkah utama:
- Extract: Membaca data dari file CSV.
- Transform: Memproses dan membersihkan data sesuai desain Star Schema.
- Load: Memuat data hasil transformasi ke database PostgreSQL.

Pipeline ini dijalankan secara otomatis menggunakan Apache Airflow dengan pendekatan batch processing. DAG akan mengeksekusi task secara berurutan: Extract → Transform → Load.

---

## **Visualisasi di Metabase**

Dashboard di Metabase menampilkan beberapa analisis utama:

- Pendapatan Bulanan: Tren pendapatan, pesanan, dan biaya pengiriman e-commerce per bulan.
- Rata-rata Pengiriman: Analisis waktu dan biaya pengiriman berdasarkan kategori produk setiap bulan.
- Churn Rate: Segmentasi pelanggan dan identifikasi pelanggan yang berhenti menggunakan layanan.
- Produk Terlaris: Daftar produk dengan pesanan tertinggi berdasarkan kategori dan pendapatan.

