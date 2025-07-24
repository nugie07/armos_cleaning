# Order Cleaning Application

Aplikasi Python untuk memperbaiki data order yang hilang antara Database A (main database) dan Database B (warehouse cleaning database).

## Fitur Utama

1. **Perbandingan Data**: Membandingkan data order antara Database A dan Database B berdasarkan rentang tanggal
2. **Deteksi Discrepancy**: Menemukan perbedaan jumlah item antara kedua database
3. **Pembuatan Payload**: Membuat payload JSON sesuai format yang diperlukan untuk memperbaiki data
4. **Penyimpanan Hasil**: Menyimpan hasil cleaning di Database B untuk tracking
5. **API REST**: Endpoint API untuk integrasi dengan aplikasi lain
6. **CLI Interface**: Command line interface untuk operasi manual

## Struktur Database

### Database A (Main Database)
- **Table `order`**: Data order utama
- **Table `order_detail`**: Detail item untuk setiap order

### Database B (Warehouse Cleaning Database)
- **Table `cleansed_outbound_documents`**: Referensi dokumen order
- **Table `cleansed_outbound_items`**: Referensi item order
- **Table `cleansed_outbound_conversions`**: Referensi konversi unit
- **Table `cleaning_payload_results`**: Hasil payload cleaning

## Instalasi

1. **Clone repository**
```bash
git clone <repository-url>
cd ordercleaning
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Setup environment variables**
```bash
cp env_example.txt .env
# Edit .env file dengan konfigurasi database Anda
```

4. **Create database tables**
```bash
python scripts/create_tables.py
```

## Konfigurasi Database

Edit file `.env` dengan konfigurasi database Anda:

```env
# Database A (Main Database)
DB_A_HOST=localhost
DB_A_PORT=5432
DB_A_NAME=main_database
DB_A_USER=your_username
DB_A_PASSWORD=your_password

# Database B (Warehouse Cleaning Database)
DB_B_HOST=localhost
DB_B_PORT=5432
DB_B_NAME=warehouse_cleaning
DB_B_USER=your_username
DB_B_PASSWORD=your_password
```

## Penggunaan

### 1. Menjalankan API Server

```bash
python api/main.py
```

Server akan berjalan di `http://localhost:8000`

### 2. Menggunakan CLI Interface

```bash
# Mode interaktif
python cli/main.py interactive-mode

# Compare data by date range
python cli/main.py compare-data --start-date 2025-01-01 --end-date 2025-01-31

# Create payload for specific do_number
python cli/main.py create-payload B01SI2507-1602

# List payload results
python cli/main.py list-payloads --limit 10

# Get specific payload
python cli/main.py get-payload B01SI2507-1602
```

### 3. API Endpoints

#### Compare Data
```bash
POST /compare-data
Content-Type: application/json

{
  "start_date": "2025-01-01",
  "end_date": "2025-01-31"
}
```

#### Create Payload
```bash
POST /create-payload/{do_number}
```

#### Get Payload Results
```bash
GET /payload-results?limit=100&offset=0
```

#### Get Specific Payload
```bash
GET /payload-result/{do_number}
```

## Alur Kerja

1. **Perbandingan Data**:
   - Input rentang tanggal (start_date, end_date)
   - Query data dari Database A berdasarkan `faktur_date`
   - Query data dari Database B berdasarkan `faktur_date`
   - Bandingkan jumlah item per `do_number`
   - Tampilkan discrepancy yang ditemukan

2. **Pembuatan Payload**:
   - Pilih `do_number` yang memiliki discrepancy
   - Ambil data lengkap dari Database B
   - Buat payload JSON sesuai format yang diperlukan
   - Simpan hasil ke table `cleaning_payload_results`

3. **Format Payload**:
```json
{
  "warehouse_id": "KJR01",
  "client_id": "BBM",
  "outbound_reference": "B01SI2507-1602",
  "divisi": "SOFT HEAVEN",
  "faktur_date": "2025-07-17",
  "request_delivery_date": "2025-07-24",
  "origin_name": "BBM-SURABAYA",
  "origin_address_1": "SURABAYA",
  "origin_address_2": "",
  "origin_city": "SURABAYA",
  "origin_phone": "",
  "origin_email": "",
  "destination_id": "CL34551",
  "destination_name": "SONIA MM",
  "destination_address_1": "DEMAK JAYA IV/8, KEC. BUBUTAN",
  "destination_address_2": "DEMAK JAYA IV/8, KEC. BUBUTAN",
  "destination_city": "SURABAYA",
  "destination_zip_code": "",
  "destination_phone": "",
  "destination_email": "",
  "order_type": "REG",
  "items": [
    {
      "warehouse_id": "KJR01",
      "line_id": "1",
      "product_id": "61396901100001",
      "product_description": "SOFT HEAVEN KAPAS 96PCX50GR",
      "group_id": "10",
      "group_description": "FACIAL TREATMENT",
      "product_type": "61",
      "qty": 3,
      "uom": "CTN",
      "pack_id": "0102",
      "product_net_price": 2066400,
      "conversion": [
        {
          "uom": "PCS",
          "numerator": 1,
          "denominator": 1
        },
        {
          "uom": "CTN",
          "numerator": 96,
          "denominator": 1
        }
      ],
      "image_url": [""]
    }
  ]
}
```

## Struktur Proyek

```
ordercleaning/
├── api/
│   └── main.py                 # FastAPI application
├── cli/
│   └── main.py                 # CLI interface
├── config/
│   └── database.py             # Database configuration
├── models/
│   ├── database_a_models.py    # Database A models
│   └── database_b_models.py    # Database B models
├── schemas/
│   └── payload_schemas.py      # Pydantic schemas
├── services/
│   └── data_comparison_service.py  # Business logic
├── scripts/
│   └── create_tables.py        # Table creation script
├── requirements.txt            # Python dependencies
├── env_example.txt            # Environment variables example
└── README.md                  # Documentation
```

## Troubleshooting

### Error Database Connection
- Pastikan konfigurasi database di `.env` sudah benar
- Pastikan database server berjalan
- Pastikan user database memiliki permission yang cukup

### Error Table Not Found
- Jalankan script `python scripts/create_tables.py`
- Pastikan nama table sesuai dengan yang ada di database

### Error Import Module
- Pastikan semua dependencies sudah terinstall: `pip install -r requirements.txt`
- Pastikan Python path sudah benar

## Kontribusi

1. Fork repository
2. Buat feature branch
3. Commit perubahan
4. Push ke branch
5. Buat Pull Request

## Lisensi

MIT License 