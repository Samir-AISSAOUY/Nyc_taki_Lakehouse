# NYC Taxi Data Download

## Required Files

The project needs **13 files** (~5 GB total):

* 12 monthly Parquet files (2023)
* 1 CSV zone lookup file

---

## Method 1: Direct Links (Recommended)

### 2023 Taxi Data (12 files)

**Right-click → Save As** into the folder `data/raw/`

1. [yellow_tripdata_2023-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet) (~450 MB)
2. [yellow_tripdata_2023-02.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet) (~400 MB)
3. [yellow_tripdata_2023-03.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet) (~450 MB)
4. [yellow_tripdata_2023-04.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet) (~430 MB)
5. [yellow_tripdata_2023-05.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet) (~440 MB)
6. [yellow_tripdata_2023-06.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-06.parquet) (~420 MB)
7. [yellow_tripdata_2023-07.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-07.parquet) (~410 MB)
8. [yellow_tripdata_2023-08.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-08.parquet) (~400 MB)
9. [yellow_tripdata_2023-09.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet) (~410 MB)
10. [yellow_tripdata_2023-10.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-10.parquet) (~430 MB)
11. [yellow_tripdata_2023-11.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet) (~420 MB)
12. [yellow_tripdata_2023-12.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet) (~430 MB)

### Zone Lookup (1 file)

13. [taxi_zone_lookup.csv](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) (~12 KB)

---

## Final Folder Structure

After download, your `data/raw/` folder should look like this:

```
data/raw/
├── yellow_tripdata_2023-01.parquet
├── yellow_tripdata_2023-02.parquet
├── yellow_tripdata_2023-03.parquet
├── yellow_tripdata_2023-04.parquet
├── yellow_tripdata_2023-05.parquet
├── yellow_tripdata_2023-06.parquet
├── yellow_tripdata_2023-07.parquet
├── yellow_tripdata_2023-08.parquet
├── yellow_tripdata_2023-09.parquet
├── yellow_tripdata_2023-10.parquet
├── yellow_tripdata_2023-11.parquet
├── yellow_tripdata_2023-12.parquet
└── taxi_zone_lookup.csv
```

**Total: 13 files (~5 GB)**

---

## Method 2: Command Line

### Windows (PowerShell)

```powershell
New-Item -ItemType Directory -Path "data/raw" -Force

$base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
1..12 | ForEach-Object {
    $month = $_.ToString("00")
    $file = "yellow_tripdata_2023-$month.parquet"
    Write-Host "Downloading $file..."
    Invoke-WebRequest -Uri "$base/$file" -OutFile "data/raw/$file"
}

Invoke-WebRequest -Uri "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" -OutFile "data/raw/taxi_zone_lookup.csv"
```

### Linux / macOS

```bash
mkdir -p data/raw

for i in {01..12}; do
    echo "Downloading yellow_tripdata_2023-$i.parquet..."
    wget -P data/raw "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-$i.parquet"
done

wget -P data/raw "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
```

### Using cURL (cross-platform)

```bash
mkdir -p data/raw

for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
    curl -o "data/raw/yellow_tripdata_2023-$month.parquet" \
         "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-$month.parquet"
done

curl -o "data/raw/taxi_zone_lookup.csv" \
     "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
```

---

## Verification

### Count files

**Windows:**

```powershell
(Get-ChildItem data/raw).Count
# Should return: 13
```

**Linux/macOS:**

```bash
ls data/raw | wc -l
# Should return: 13
```

### Check total size

**Windows:**

```powershell
$size = (Get-ChildItem data/raw -Recurse | Measure-Object -Property Length -Sum).Sum / 1GB
Write-Host "Total: $([math]::Round($size, 2)) GB"
```

**Linux/macOS:**

```bash
du -sh data/raw
# Should show: ~5.0G
```

---

## After Download

Once all 13 files are downloaded:

```bash
docker compose up -d
sleep 120
docker exec airflow airflow dags trigger nyc_taxi_lakehouse
```

---

## Data Source

**NYC Taxi & Limousine Commission (TLC)**

* Official site: [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
* License: Public data
* Format: Parquet (Apache)
* Period: January–December 2023

---

## Important Notes

1. **Size:** Large files (~5 GB total). Make sure you have:

   * Stable internet connection
   * Enough disk space (50 GB recommended)

2. **Time:** Downloads can take 20–60 minutes depending on your connection.

3. **Gitignore:** `.parquet` files are excluded from Git repositories.

4. **If downloads fail:**

   * Use a download manager (IDM, Free Download Manager)
   * Download one file at a time
   * Use a VPN if URLs are blocked

---

## Common Issues

### "Connection timeout"

**Fix:**

```powershell
Invoke-WebRequest -Uri $url -OutFile $file -TimeoutSec 600
```

### "Access denied"

**Fix:** Check URLs or use a VPN.

### Corrupted files

**Fix:**

```powershell
Remove-Item data/raw/yellow_tripdata_2023-XX.parquet
Invoke-WebRequest -Uri $url -OutFile data/raw/yellow_tripdata_2023-XX.parquet
```

---
