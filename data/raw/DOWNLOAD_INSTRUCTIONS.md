# 📥 Téléchargement des Données NYC Taxi

## 🎯 Fichiers Requis

Le projet nécessite **13 fichiers** (~5 GB total) :
- 12 fichiers Parquet mensuels (2023)
- 1 fichier CSV zones lookup

---

## 🔗 Méthode 1 : Liens Directs (Recommandé)

### Données Taxi 2023 (12 fichiers)

**Clic droit → Enregistrer sous** dans le dossier `data/raw/`

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

### Zone Lookup (1 fichier)

13. [taxi_zone_lookup.csv](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) (~12 KB)

---

## 📂 Structure Finale

Après téléchargement, votre dossier `data/raw/` doit contenir :

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

**Total : 13 fichiers (~5 GB)**

---

## 🖥️ Méthode 2 : Ligne de Commande

### Windows (PowerShell)

```powershell
# Créer le dossier
New-Item -ItemType Directory -Path "data/raw" -Force

# Télécharger tous les fichiers
$base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
1..12 | ForEach-Object {
    $month = $_.ToString("00")
    $file = "yellow_tripdata_2023-$month.parquet"
    Write-Host "Downloading $file..."
    Invoke-WebRequest -Uri "$base/$file" -OutFile "data/raw/$file"
}

# Zone lookup
Invoke-WebRequest -Uri "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv" -OutFile "data/raw/taxi_zone_lookup.csv"
```

### Linux / macOS

```bash
# Créer le dossier
mkdir -p data/raw

# Télécharger tous les fichiers
for i in {01..12}; do
    echo "Downloading yellow_tripdata_2023-$i.parquet..."
    wget -P data/raw "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-$i.parquet"
done

# Zone lookup
wget -P data/raw "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
```

### Avec cURL (multi-plateforme)

```bash
# Créer le dossier
mkdir -p data/raw

# Télécharger
for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
    curl -o "data/raw/yellow_tripdata_2023-$month.parquet" \
         "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-$month.parquet"
done

# Zone lookup
curl -o "data/raw/taxi_zone_lookup.csv" \
     "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
```

---

## 🔍 Vérification

### Compter les fichiers

**Windows :**
```powershell
(Get-ChildItem data/raw).Count
# Doit retourner: 13
```

**Linux/macOS :**
```bash
ls data/raw | wc -l
# Doit retourner: 13
```

### Vérifier la taille totale

**Windows :**
```powershell
$size = (Get-ChildItem data/raw -Recurse | Measure-Object -Property Length -Sum).Sum / 1GB
Write-Host "Total: $([math]::Round($size, 2)) GB"
```

**Linux/macOS :**
```bash
du -sh data/raw
# Doit afficher: ~5.0G
```

---

## 🚀 Après le Téléchargement

Une fois les 13 fichiers téléchargés :

```bash
# Démarrer l'infrastructure
docker compose up -d

# Attendre 2 minutes
sleep 120

# Lancer le pipeline
docker exec airflow airflow dags trigger nyc_taxi_lakehouse
```

---

## 📚 Source des Données

**NYC Taxi & Limousine Commission (TLC)**
- Site officiel : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Licence : Données publiques
- Format : Parquet (Apache)
- Période : Janvier - Décembre 2023

---

## ⚠️ Notes Importantes

1. **Taille** : Les fichiers sont volumineux (~5 GB total). Assurez-vous d'avoir :
   - Connexion Internet stable
   - Espace disque suffisant (50 GB recommandé)

2. **Durée** : Le téléchargement peut prendre 20-60 minutes selon votre connexion

3. **Gitignore** : Les fichiers .parquet sont exclus du repository Git (trop volumineux)

4. **Alternative** : Si les téléchargements échouent, essayez :
   - Un gestionnaire de téléchargement (IDM, Free Download Manager)
   - Télécharger un fichier à la fois
   - Utiliser un VPN si les URLs sont bloquées

---

## 🆘 Problèmes Fréquents

### Erreur : "Connection timeout"

**Solution :** Augmenter le timeout
```powershell
Invoke-WebRequest -Uri $url -OutFile $file -TimeoutSec 600
```

### Erreur : "Access denied"

**Solution :** Vérifier que les URLs sont correctes ou utiliser un VPN

### Fichiers corrompus

**Solution :** Retélécharger le fichier spécifique
```powershell
Remove-Item data/raw/yellow_tripdata_2023-XX.parquet
Invoke-WebRequest -Uri $url -OutFile data/raw/yellow_tripdata_2023-XX.parquet
```

---

**Bon téléchargement ! 📥**