# UTS Pemrograman untuk Big Data: PySpark RDD & Spark SQL

Repositori ini berisi dua modul praktikum UTS mata kuliah **Pemrograman Untuk Big Data**, yang keduanya dijalankan di **Google Colab** menggunakan **PySpark** (mode lokal `local[*]`).

| | |
|---|---|
| **Nama** | Isnaini Nur Hasanah |
| **NIM** | 52240005 |
| **Program Studi** | Sains Data 2024 |
| **Mata Kuliah** | Pemrograman Untuk Big Data |

---

## Daftar Isi

1. [Gambaran Umum](#gambaran-umum)
2. [Struktur File](#struktur-file)
3. [Prasyarat & Cara Menjalankan](#prasyarat--cara-menjalankan)
4. [Modul 1: Pemrosesan Terdistribusi dengan PySpark RDD](#modul-1-pemrosesan-terdistribusi-dengan-pyspark-rdd)
5. [Modul 2: PySpark DataFrame API & Spark SQL](#modul-2-pyspark-dataframe-api--spark-sql)
6. [Perbandingan Kedua Modul](#perbandingan-kedua-modul)
7. [Catatan Penting](#catatan-penting)

---

## Gambaran Umum

| Aspek | Modul 1 | Modul 2 |
|---|---|---|
| Topik | Pemrosesan terdistribusi dengan **RDD** | Pemrosesan terstruktur dengan **DataFrame API & Spark SQL** |
| Dataset | UCI **Online Retail** (`id=352`) | **NYC Yellow Taxi**, Januari 2015 |
| Cara memperoleh data | `ucimlrepo` (`fetch_ucirepo`) | Unduh via `gdown` dari Google Drive |
| Domain | Transaksi ritel online (GBP) | Perjalanan taksi kota (USD) |
| Fokus | `filter`, `map`, `reduceByKey`, caching, partisi, set operation | Explicit schema, `groupBy`/`agg`, `CASE WHEN`, window function, Parquet |

## Struktur File

```
.
├── UTS_Pemrograman_Modul1_RDD_Isnaini.ipynb   # Modul 1: PySpark RDD (Online Retail)
├── UTS_Pemrograman_Modul2_SQL_Isnaini.ipynb   # Modul 2: DataFrame & Spark SQL (NYC Taxi)
└── README.md                                  # Dokumen ini
```

## Prasyarat & Cara Menjalankan

**Lingkungan:** Google Colab (Python 3, runtime standar sudah cukup).

**Library** (dipasang otomatis di sel pertama masing-masing notebook):

- Modul 1: `pyspark`, `ucimlrepo`, `pandas`, `matplotlib`
- Modul 2: `pyspark`, `gdown`

**Langkah:**

1. Unggah notebook ke Google Colab (atau buka lewat Google Drive).
2. Jalankan sel secara berurutan dari atas (**Runtime → Run all**).
3. Modul 1 mengunduh dataset otomatis dari UCI dan menyimpannya sebagai `/content/online_retail.csv`.
4. Modul 2 mengunduh file `yellow_tripdata_2015-01.csv` (berukuran > 1 GB) ke `/content/`. Unduhan dilewati jika file sudah ada.
5. Modul 2 membutuhkan memori yang cukup besar (`spark.driver.memory = 8g`), jadi pastikan runtime Colab memiliki RAM memadai.

> Jalankan kedua modul pada sesi yang berbeda, atau pastikan `spark.stop()` di akhir Modul 1 sudah dieksekusi sebelum memulai Modul 2.

---

## Modul 1: Pemrosesan Terdistribusi dengan PySpark RDD

**File:** `UTS_Pemrograman_Modul1_RDD_Isnaini.ipynb`

### Dataset

UCI Online Retail dengan 8 kolom: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`. Kolom ID digabung dengan fitur, dirapikan tipe datanya, lalu disimpan sebagai CSV untuk dibaca Spark.

### Alur Pengerjaan

| Bagian | Isi |
|---|---|
| 1 | Instalasi library & pengambilan dataset via `ucimlrepo` |
| 2 | Inisialisasi dan verifikasi `SparkContext` |
| 3 | Membaca data, membersihkan nilai NULL, mengubah ke RDD tuple, `repartition(8)` |
| 4 | Transformasi `filter` & `map` (transaksi valid United Kingdom) |
| 5 | Agregasi `reduceByKey` dan sorting (Top 10 invoice) |
| 6 | Optimasi dengan caching/persistence (`StorageLevel.MEMORY_ONLY`) |
| 7 | Analisis produk paling laris |
| 8 | Tugas mandiri (kategori A sampai D, lihat di bawah) |

### Tugas Mandiri & Temuan Utama

| Tugas | Deskripsi | Temuan utama |
|---|---|---|
| **A1** | Top 5 pelanggan non-UK | Pendapatan ekspor sangat terkonsentrasi. CustomerID 14646 teratas (£280.206,02), hampir 2x pelanggan peringkat 2. |
| **A2** | Potensi kerugian pembatalan/retur | 8.905 baris di 3.654 invoice unik, estimasi nilai kotor £611.342,09. |
| **B1** | Pengaruh jumlah partisi (2, 8, 16) | Waktu justru naik seiring partisi bertambah, karena mode lokal hanya menambah overhead shuffle dan penjadwalan task. |
| **B2** | `reduceByKey` vs `groupByKey` | Hasil identik. Selisih waktu masih dalam batas noise pada mode lokal, tetapi `reduceByKey` tetap lebih efisien di cluster nyata. |
| **C1** | Jam sibuk transaksi | Puncak pukul 12:00–12:59, lalu 13:00 dan 14:00. Distribusi berbentuk lonceng. |
| **C2** | Rata-rata harga produk "HEART" | 297 varian, harga rata-rata berkisar £0,19 sampai £35,65. |
| **D1** | Produk eksklusif Germany vs France (`subtract`) | 514 produk Germany tidak pernah dibeli di France (sekitar 30%). |

---

## Modul 2: PySpark DataFrame API & Spark SQL

**File:** `UTS_Pemrograman_Modul2_SQL_Isnaini.ipynb`

### Dataset

NYC Yellow Taxi (Januari 2015) dengan **19 kolom**, di antaranya `vendor_id`, `pickup_datetime`, `dropoff_datetime`, `passenger_count`, `trip_distance`, `payment_type`, `fare_amount`, `tip_amount`, dan `total_amount`.

### Alur Pengerjaan

| Bagian | Isi |
|---|---|
| 0 | Setup PySpark di Colab |
| 1 | Unduh dataset NYC Taxi |
| 2 | Ingestion dengan **explicit schema** (`StructType`) dan validasi jumlah kolom |
| 3 | Pembersihan data (`trip_distance > 0`, `total_amount > 0`) & feature engineering (`pickup_hour`, `trip_duration_min`), lalu `createOrReplaceTempView("v_nyc_taxi")` |
| 4 | Contoh agregasi DataFrame API per jam |
| 5 | Tugas mandiri (kategori A sampai C, lihat di bawah) |

### Tugas Mandiri & Temuan Utama

| Tugas | Deskripsi | Temuan utama |
|---|---|---|
| **A1** | Jam sibuk (DataFrame API) | Jam 19:00 tersibuk (800.980 perjalanan). Top 5 semuanya jatuh pada 18:00–22:00. |
| **A2** | Durasi berdasarkan jumlah penumpang | Sekitar 70% perjalanan berpenumpang 1 orang. Durasi dan tarif hampir sama untuk 1–6 penumpang. |
| **B1** | Segmentasi jarak (`CASE WHEN`) | Short ± 59%, Medium ± 36%, Long ± 4,6%. Tarif dan tip naik seiring jarak. |
| **B2** | Metode pembayaran & tip rate | Kartu kredit (tipe 1) tip tertinggi, 15,15%. Tip tunai tercatat 0% karena memang tidak terekam di sistem. |
| **C1** | Ranking jarak per jam (`dense_rank`) | Rank 1 pada jam 08, 12, 18 berisi data tidak valid (jarak mustahil). |
| **C2** | Export hasil A1 ke Parquet | Tersimpan 24 baris (1 per jam) di `/content/output/nyc_hourly_summary.parquet` dan terverifikasi saat dibaca ulang. |

---

## Perbandingan Kedua Modul

| Aspek | RDD (Modul 1) | DataFrame/SQL (Modul 2) |
|---|---|---|
| Abstraksi | Low-level, tuple & lambda | High-level, kolom bernama & skema |
| Skema | Ditentukan manual lewat posisi tuple | Explicit schema (`StructType`) |
| Gaya penulisan | Fungsional (`map`, `filter`, `reduceByKey`) | Deklaratif (`groupBy`, `agg`, SQL) |
| Cocok untuk | Kontrol penuh atas transformasi dan data tidak terstruktur | Analitik data terstruktur dalam skala besar |
| Penyimpanan hasil | Ditampilkan di notebook | Parquet (columnar & terkompresi) |

## Catatan Penting

- **Mode lokal Colab:** pengukuran waktu pada Modul 1 (partisi dan `reduceByKey` vs `groupByKey`) dilakukan di satu mesin dengan sedikit core, sehingga hasilnya tidak mencerminkan performa pada cluster terdistribusi sesungguhnya.
- **Kualitas data NYC Taxi:** terdapat outlier ekstrem pada `trip_distance` (misalnya jutaan mil), yang membuat `avg_distance` pada A1 tidak masuk akal. Pada analisis sungguhan, data perlu difilter dengan batas wajar sebelum agregasi.
- **Penyesuaian soal B1 (Modul 2):** batas segmen *Medium* memakai `> 2.0 AND <= 10.0` (bukan `BETWEEN 2.1 AND 10.0`) agar tidak ada jarak yang jatuh ke celah dan bernilai `NULL`.
- **Kualitas data Online Retail:** sebagian `Description` memiliki spasi di awal, sehingga bisa dianggap berbeda saat `distinct()`/`subtract()`. Menambahkan `.strip()` dapat sedikit mengubah hasil D1.
- Angka pada ringkasan di atas diambil dari output saat notebook dijalankan, sehingga dapat sedikit berbeda jika dijalankan ulang (terutama nilai waktu eksekusi).
