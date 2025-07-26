# Database Operations Script

Script untuk operasi database yang mencakup pembuatan tabel, copy data order, dan copy data product dari Database A ke Database B.

## Fitur

1. **Environment Configuration** - Pengaturan untuk 2 database (A dan B)
2. **Table Creation** - Pembuatan tabel baru di database B
3. **Data Copy** - Copy data dengan range tanggal untuk order dan order_detail
4. **Product Sync** - Copy data mst_product ke mst_product_main
5. **Comprehensive Logging** - Log detail untuk setiap proses
6. **Error Handling** - Retry mechanism dan error recovery
7. **Batch Processing** - Processing data dalam batch untuk performa optimal
8. **UPSERT Mechanism** - Handle perubahan data dengan update existing records

## Konsep UPSERT

### Initial Copy vs UPSERT

**Initial Copy (DO NOTHING):**
- Script: `copy_product_data.py`, `copy_order_data.py`
- Behavior: Skip data yang sudah ada (tidak duplikasi)
- Use case: Copy data pertama kali

**UPSERT (DO UPDATE):**
- Script: `copy_product_data_upsert.py`, `copy_order_data_upsert.py`
- Behavior: Update data yang sudah ada jika ada perubahan
- Use case: Sync perubahan data dari source ke target

### Strategi Penggunaan

1. **Pertama kali:** Gunakan Initial Copy untuk load semua data
2. **Selanjutnya:** Gunakan UPSERT untuk sync perubahan
3. **Rutin:** Jalankan UPSERT secara berkala untuk update data

### Warehouse Filter

**Order Data:**
- Filter berdasarkan `warehouse_id` untuk copy data order dan order_detail
- Order detail otomatis ter-filter berdasarkan order yang sudah di-filter
- Format: `--warehouse-id WAREHOUSE_001`

**Product Data:**
- Tidak ada filter warehouse (copy semua product)
- Product data bersifat global untuk semua warehouse

## Struktur Tabel

### order_main
- order_id (SERIAL PRIMARY KEY)
- faktur_id, faktur_date, delivery_date, do_number, status
- skip_count, created_date, created_by, updated_date, updated_by
- notes, customer_id, warehouse_id, delivery_type_id, order_integration_id
- origin_name, origin_address_1, origin_address_2, origin_city, origin_zipcode
- origin_phone, origin_email, destination_name, destination_address_1
- destination_address_2, destination_city, destination_zip_code
- destination_phone, destination_email, client_id, cancel_reason
- rdo_integration_id, address_change, divisi, pre_status

### order_detail_main
- order_detail_id (SERIAL PRIMARY KEY)
- quantity_faktur, net_price, quantity_wms, quantity_delivery
- quantity_loading, quantity_unloading, status, cancel_reason, notes
- order_id (FOREIGN KEY), product_id, unit_id, pack_id, line_id
- unloading_latitude, unloading_longitude, origin_uom, origin_qty
- total_ctn, total_pcs

### mst_product_main
- mst_product_id (SERIAL PRIMARY KEY)
- sku (UNIQUE), height, width, length, name, price
- type_product_id, qty, volume, weight, base_uom
- pack_id, warehouse_id, synced_at, allocated_qty, available_qty

## Instalasi

### Prerequisites
- Python 3.7+
- pip3
- PostgreSQL database access

### Setup

1. **Clone atau download project ini**

2. **Install dependencies**
   ```bash
   pip3 install -r requirements.txt
   ```

3. **Konfigurasi database**
   Edit file `config.env` dengan konfigurasi database Anda:
   ```env
   # Database A Configuration
   DB_A_HOST=localhost
   DB_A_PORT=5432
   DB_A_NAME=database_a
   DB_A_USER=username_a
   DB_A_PASSWORD=password_a

   # Database B Configuration
   DB_B_HOST=localhost
   DB_B_PORT=5432
   DB_B_NAME=database_b
   DB_B_USER=username_b
   DB_B_PASSWORD=password_b

   # Logging Configuration
   LOG_LEVEL=INFO
   LOG_FILE=./logs/database_operations.log

   # Application Configuration
   BATCH_SIZE=1000
   MAX_RETRIES=3
   RETRY_DELAY=5
   ```

## Penggunaan

### Menggunakan Shell Script (Recommended)

```bash
# Setup environment dan buat tabel
./run_database_operations.sh --setup-only

# Copy product data (Initial Copy - skip existing)
./run_database_operations.sh --copy-products

# Copy product data dengan batch custom (10k records, delay 30s)
./run_database_operations.sh --copy-products-batch 10000 30

# Copy product data dengan UPSERT (update existing)
./run_database_operations.sh --copy-products-upsert

# Copy product data dengan UPSERT dan batch custom
./run_database_operations.sh --copy-products-upsert-batch 10000 30

# Copy order data dengan range tanggal dan warehouse filter
./run_database_operations.sh --copy-orders 2024-01-01 2024-01-31 WAREHOUSE_001

# Copy semua data (product + order dengan range tanggal dan warehouse filter)
./run_database_operations.sh --copy-all 2024-01-01 2024-01-31 WAREHOUSE_001

# Lihat help
./run_database_operations.sh --help
```

### Menggunakan Python Script Langsung

```bash
# Buat tabel
python3 create_tables.py

# Copy product data (Initial Copy - skip existing)
python3 copy_product_data.py --validate

# Copy product data dengan batch custom (10k records, delay 30s)
python3 copy_product_data.py --validate --batch-size 10000 --batch-delay 30

# Copy product data dengan UPSERT (update existing)
python3 copy_product_data_upsert.py --validate

# Copy product data dengan UPSERT dan batch custom
python3 copy_product_data_upsert.py --validate --batch-size 10000 --batch-delay 30

# Copy order data dengan warehouse filter
python3 copy_order_data.py --start-date 2024-01-01 --end-date 2024-01-31 --warehouse-id WAREHOUSE_001

# Copy order data dengan UPSERT dan warehouse filter
python3 copy_order_data_upsert.py --start-date 2024-01-01 --end-date 2024-01-31 --warehouse-id WAREHOUSE_001
```

## Logging

Semua operasi akan di-log ke:
- Console output (real-time)
- File log: `./logs/database_operations.log`

Level log dapat diatur di `config.env`:
- DEBUG: Detail lengkap
- INFO: Informasi umum (default)
- WARNING: Peringatan
- ERROR: Error saja

## Konfigurasi

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| BATCH_SIZE | Jumlah record per batch | 1000 |
| MAX_RETRIES | Maksimal retry jika gagal | 3 |
| RETRY_DELAY | Delay antar retry (detik) | 5 |
| LOG_LEVEL | Level logging | INFO |

### Database Connection

Pastikan database A memiliki tabel:
- `order`
- `order_detail` 
- `mst_product`

Dan database B akan dibuat tabel:
- `order_main`
- `order_detail_main`
- `mst_product_main`

## Troubleshooting

### Error: Connection Failed
- Periksa konfigurasi database di `config.env`
- Pastikan database server berjalan
- Periksa firewall dan network connectivity

### Error: Table Already Exists
- Script menggunakan `CREATE TABLE IF NOT EXISTS`, jadi aman untuk dijalankan berulang
- Jika ingin recreate tabel, drop manual dulu

### Error: Permission Denied
- Pastikan user database memiliki permission untuk:
  - SELECT pada database A
  - CREATE, INSERT, UPDATE pada database B

### Error: Data Type Mismatch
- Periksa struktur tabel source dan target
- Pastikan data type compatible

### Performance Issues
- Kurangi `BATCH_SIZE` jika memory terbatas
- Tingkatkan `BATCH_SIZE` jika network cepat
- Monitor log untuk progress

## Monitoring

### Progress Tracking
Script akan menampilkan progress real-time:
```
[2024-01-15 10:30:15] Found 5000 order records to copy
[2024-01-15 10:30:16] Copied 1000 order records (Total: 1000/5000)
[2024-01-15 10:30:17] Copied 1000 order records (Total: 2000/5000)
```

### Validation
Gunakan flag `--validate` untuk memverifikasi data:
```
[2024-01-15 10:35:20] Source database product count: 1500
[2024-01-15 10:35:21] Target database product count: 1500
[2024-01-15 10:35:21] Product data validation successful - counts match
```

## Backup dan Recovery

### Sebelum Menjalankan Script
```sql
-- Backup database B
pg_dump database_b > backup_before_copy.sql
```

### Jika Gagal
```sql
-- Restore database B
psql database_b < backup_before_copy.sql
```

## Support

Jika ada masalah atau pertanyaan:
1. Periksa log file untuk detail error
2. Pastikan semua prerequisites terpenuhi
3. Test koneksi database manual
4. Periksa permission database user 