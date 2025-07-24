# Panduan Penggunaan Order Cleaning Application

## Persiapan Awal

### 1. Setup Environment
```bash
# Copy file environment
cp env_example.txt .env

# Edit file .env dengan konfigurasi database Anda
nano .env
```

### 2. Install Dependencies
```bash
# Menggunakan script otomatis
./start.sh

# Atau manual
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Create Database Tables
```bash
python scripts/create_tables.py
```

## Cara Penggunaan

### Mode 1: API Server

#### Menjalankan Server
```bash
# Menggunakan script
./start.sh

# Atau manual
python run.py
```

Server akan berjalan di `http://localhost:8000`

#### Mengakses API Documentation
Buka browser dan akses: `http://localhost:8000/docs`

#### Contoh Penggunaan API

**1. Compare Data**
```bash
curl -X POST "http://localhost:8000/compare-data" \
  -H "Content-Type: application/json" \
  -d '{
    "start_date": "2025-01-01",
    "end_date": "2025-01-31"
  }'
```

**2. Create Payload**
```bash
curl -X POST "http://localhost:8000/create-payload/B01SI2507-1602"
```

**3. Get Payload Results**
```bash
curl -X GET "http://localhost:8000/payload-results?limit=10&offset=0"
```

### Mode 2: CLI Interface

#### Menjalankan CLI
```bash
# Mode interaktif
./cli_tool.sh interactive-mode

# Compare data
./cli_tool.sh compare-data --start-date 2025-01-01 --end-date 2025-01-31

# Create payload
./cli_tool.sh create-payload B01SI2507-1602

# List payloads
./cli_tool.sh list-payloads --limit 10

# Get specific payload
./cli_tool.sh get-payload B01SI2507-1602
```

#### Mode Interaktif
```bash
./cli_tool.sh interactive-mode
```

Dalam mode interaktif, Anda akan melihat menu:
```
Available options:
1. Compare data by date range
2. Create payload for specific do_number
3. List payload results
4. Get specific payload
5. Exit
```

## Alur Kerja Lengkap

### Langkah 1: Perbandingan Data
1. Jalankan perbandingan data dengan rentang tanggal
2. Sistem akan menampilkan discrepancy yang ditemukan
3. Contoh output:
```
Found 5 discrepancies between Database A and Database B
Total discrepancies: 5

Discrepancies found:
1. B01SI2507-1602 - DB A: 3, DB B: 8, Diff: 5
2. B01SI2508-1603 - DB A: 2, DB B: 6, Diff: 4
3. B01SI2509-1604 - DB A: 1, DB B: 4, Diff: 3
```

### Langkah 2: Pembuatan Payload
1. Pilih do_number yang memiliki discrepancy
2. Sistem akan membuat payload dari data Database B
3. Payload akan disimpan di table `cleaning_payload_results`
4. File JSON akan dibuat secara otomatis

### Langkah 3: Verifikasi Hasil
1. Cek hasil payload yang telah dibuat
2. Verifikasi format payload sesuai kebutuhan
3. Gunakan payload untuk memperbaiki data di Database A

## Troubleshooting

### Error Database Connection
```
Error: .env file not found!
Please copy env_example.txt to .env and configure your database settings.
```
**Solusi**: Pastikan file `.env` sudah dibuat dan dikonfigurasi dengan benar.

### Error Table Not Found
```
Error creating Database A tables: (psycopg2.errors.UndefinedTable)
```
**Solusi**: Jalankan `python scripts/create_tables.py` untuk membuat table.

### Error Import Module
```
ModuleNotFoundError: No module named 'fastapi'
```
**Solusi**: Install dependencies dengan `pip install -r requirements.txt`

### Error Permission Denied
```
Permission denied: ./start.sh
```
**Solusi**: Berikan permission execute dengan `chmod +x start.sh`

## Tips Penggunaan

### 1. Backup Database
Sebelum melakukan operasi cleaning, selalu backup database Anda.

### 2. Test dengan Data Kecil
Mulai dengan rentang tanggal yang kecil untuk testing.

### 3. Verifikasi Payload
Selalu verifikasi payload yang dihasilkan sebelum digunakan.

### 4. Monitor Logs
Perhatikan log aplikasi untuk debugging jika ada error.

### 5. Regular Maintenance
Jalankan maintenance database secara berkala.

## Contoh Skenario Penggunaan

### Skenario 1: Data Hilang di Database A
1. **Deteksi**: Compare data menemukan discrepancy
2. **Analisis**: Database A memiliki 3 item, Database B memiliki 8 item
3. **Aksi**: Buat payload dari Database B
4. **Implementasi**: Gunakan payload untuk menambahkan 5 item yang hilang

### Skenario 2: Data Duplikat
1. **Deteksi**: Compare data menemukan discrepancy
2. **Analisis**: Database A memiliki 10 item, Database B memiliki 5 item
3. **Aksi**: Buat payload dari Database B sebagai referensi
4. **Implementasi**: Hapus 5 item duplikat di Database A

### Skenario 3: Data Tidak Konsisten
1. **Deteksi**: Compare data menemukan discrepancy
2. **Analisis**: Jumlah item sama tapi data berbeda
3. **Aksi**: Buat payload dari Database B
4. **Implementasi**: Update data di Database A sesuai Database B 