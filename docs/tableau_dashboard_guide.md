# Tableau Desktop Dashboard Guide

Complete guide for creating a professional dashboard with Tableau Desktop.

## Objective

Create an interactive dashboard to analyze 50M NYC taxi trips.

## Step 1: Retrieve CSV data

### After pipeline execution

CSV files are exported to `data/tableau_exports/`:

```
data/tableau_exports/
├── kpi_monthly/           # Monthly metrics
├── daily_trends/          # Daily trends
├── hourly_heatmap/        # Hourly heatmap
├── top_routes_volume/     # Top routes by volume
├── borough_summary/       # Borough summary
├── performance_ranking/   # Best/Worst days
├── weekday_comparison/    # Weekend vs Weekday
├── peak_hours/            # Peak hours
├── route_efficiency/      # Profitable routes
└── borough_flow/          # Inter/intra borough flow
```

### Copy CSVs from Docker

```powershell
# Create local folder
mkdir tableau_data

# Copy all CSVs
docker cp spark-master:/data/tableau_exports/. tableau_data/
```

## Step 2: Import into Tableau Desktop

### 1. Open Tableau Desktop

- Download: https://www.tableau.com/products/desktop/download
- Free student version available

### 2. Connect sources

**File → Connect → Text File**

Import the following files (in order):

1. **kpi_monthly.csv** → Rename to "KPI Monthly"
2. **daily_trends.csv** → Rename to "Daily Trends"
3. **hourly_heatmap.csv** → Rename to "Hourly Heatmap"
4. **borough_summary.csv** → Rename to "Borough Summary"
5. **top_routes_volume.csv** → Rename to "Top Routes"

### 3. Create relationships

**Data → Edit Relationships**

```
KPI Monthly (year-month) ←→ Daily Trends (date)
Daily Trends (date) ←→ Hourly Heatmap (date)
Borough Summary (borough) ←→ Top Routes (PU_Borough)
```

## Step 3: Create the Dashboard

### Dashboard Structure (4 pages)

```
┌────────────────────────────────────────┐
│  Page 1: OVERVIEW (KPI Executive)      │
│  Page 2: TRENDS (Time Series)          │
│  Page 3: HEATMAP (Hourly Patterns)     │
│  Page 4: GEOGRAPHY (Borough & Routes)  │
└────────────────────────────────────────┘
```

## Page 1: OVERVIEW (KPI Executive)

### Visualizations to create

#### 1. KPI Cards (Big Numbers)

**Sheet 1: Total Trips YTD**
```
- Drag: total_trips_ytd → Text
- Format: Number (Custom) → #,##0
- Color: Blue gradient
- Font Size: 48
```

**Sheet 2: Total Revenue YTD**
```
- Drag: total_revenue_ytd → Text
- Format: Currency → $#,##0
- Color: Green gradient
- Font Size: 48
- Prefix: $
```

**Sheet 3: Average Fare**
```
- Drag: avg_fare_ytd → Text
- Format: Currency → $0.00
- Color: Orange
- Font Size: 48
```

**Sheet 4: Avg Distance**
```
- Drag: avg_distance_ytd → Text
- Format: Number → 0.00
- Suffix: miles
- Color: Purple
- Font Size: 48
```

#### 2. Monthly Revenue Trend (Line Chart)

```
- Columns: year_month
- Rows: monthly_revenue
- Mark Type: Line + Circle
- Color: Green gradient
- Add Trend Line
- Add Reference Line (Average)
```

#### 3. MoM Growth (Bar Chart)

```
- Columns: year_month
- Rows: mom_growth_pct
- Mark Type: Bar
- Color: 
  - Green if > 0
  - Red if < 0
- Add Zero Reference Line
```

#### 4. Best vs Worst Days (Table)

```
- Rows: pickup_date, rank_type
- Columns: total_trips, total_revenue
- Color: By rank_type
- Sort: By revenue DESC
```

### Dashboard Layout

```
┌──────────────────────────────────────────┐
│  [Total Trips] [Revenue] [Avg Fare] [Dist]│
├──────────────────────────────────────────┤
│  Monthly Revenue Trend Chart             │
├──────────────────────────────────────────┤
│  MoM Growth Bar  │  Best/Worst Days Table│
└──────────────────────────────────────────┘
```

## Page 2: TRENDS (Time Series)

### Visualizations

#### 1. Daily Trips with Moving Averages

```
- Columns: pickup_date
- Rows: 
  - total_trips (Line, Blue)
  - trips_ma7 (Line, Orange, dashed)
  - trips_ma30 (Line, Red, dashed)
- Dual Axis
- Add Forecast (30 days)
```

#### 2. Weekday vs Weekend Comparison

```
- Columns: is_weekend
- Rows: avg_daily_trips, avg_daily_revenue
- Mark Type: Bar
- Color: By is_weekend
  - Weekend: Orange
  - Weekday: Blue
```

#### 3. Daily Revenue Heatmap (Calendar)

```
- Columns: WEEK(pickup_date)
- Rows: WEEKDAY(pickup_date)
- Color: total_revenue (Blue-Red gradient)
- Mark Type: Square
```

## Page 3: HEATMAP (Hourly Patterns)

### Visualizations

#### 1. Hour x Day Heatmap

```
- Columns: pickup_hour (0-23)
- Rows: day_name (Sun-Sat)
- Color: avg_trips (Yellow-Red gradient)
- Mark Type: Square
- Size: avg_trips
- Tooltip: avg_fare, avg_speed
```

#### 2. Peak Hours Bar Chart

```
- Columns: pickup_hour
- Rows: total_trips_by_hour
- Mark Type: Bar
- Color: Gradient (Light to Dark Blue)
- Highlight: Top 3 hours
```

#### 3. Speed vs Hour Line Chart

```
- Columns: pickup_hour
- Rows: avg_speed
- Mark Type: Line + Area
- Color: Speed gradient
- Reference Line: City speed limit
```

## Page 4: GEOGRAPHY (Borough & Routes)

### Visualizations

#### 1. Borough Performance (Treemap)

```
- Mark Type: Treemap
- Size: total_revenue
- Color: avg_fare
- Label: PU_Borough, revenue
- Tooltip: All metrics
```

#### 2. Inter vs Intra Borough (Pie)

```
- Mark Type: Pie
- Angle: total_trips
- Color: flow_type
  - Intra-Borough: Blue
  - Inter-Borough: Orange
- Label: Percentage
```

#### 3. Top 20 Routes (Bar Chart)

```
- Rows: PU_Zone + " → " + DO_Zone (Concatenate)
- Columns: total_trips
- Mark Type: Bar (Horizontal)
- Color: total_revenue (gradient)
- Sort: By trips DESC
- Limit: Top 20
```

#### 4. Route Efficiency Scatter

```
- Columns: avg_distance
- Rows: revenue_per_mile
- Mark Type: Circle
- Size: total_trips
- Color: PU_Borough
- Tooltip: Route details
```

## Design Best Practices

### Color Palette

```
Primary Blue:   #1f77b4
Success Green:  #2ca02c
Warning Orange: #ff7f0e
Danger Red:     #d62728
Purple:         #9467bd
```

### Fonts

```
Title: Tableau Bold, 24pt
Subtitle: Tableau Regular, 16pt
Body: Tableau Book, 12pt
Numbers: Tableau Medium, 14pt
```

### Layout Tips

- Use consistent spacing (16px grid)
- Align all elements properly
- White space is your friend
- Group related visualizations
- Add clear titles and descriptions

## Animations & Interactions

### Filters

Add global filters:

1. **Date Range**: On all pages
2. **Borough**: On Geography pages
3. **Day of Week**: On Trends page
4. **Hour Range**: On Heatmap page

### Actions

**Highlight Action:**
```
Source: Any worksheet
Target: All using related fields
Run On: Hover
```

**Filter Action:**
```
Source: Borough Treemap
Target: Top Routes Chart
Run On: Select
```

**URL Action:**
```
When user clicks borough → Open Google Maps
URL: https://www.google.com/maps/search/<Borough>+NYC
```

## Responsive Design

### Dashboard Sizing

```
Desktop: 1920 x 1080 (Fixed)
Tablet: 1024 x 768 (Range)
Mobile: 768 x 1024 (Automatic)
```

### Device Layouts

Create 3 versions:
- Desktop (All visualizations)
- Tablet (Simplified, 2 columns)
- Mobile (Stacked, 1 column, key metrics only)

## Publication

### Option 1: Tableau Public (Free)

```
1. File → Save to Tableau Public
2. Share link
3. Embed in website
```

### Option 2: Export PDF

```
File → Print to PDF
- Include all pages
- High resolution
```

### Option 3: Export Images

```
Dashboard → Export Image
- Resolution: 300 DPI
- Format: PNG
```

## Dashboard Checklist

- [ ] Page 1: KPI cards created (4 metrics)
- [ ] Page 1: Monthly trend line chart
- [ ] Page 1: MoM growth bars
- [ ] Page 1: Best/Worst days table
- [ ] Page 2: Daily trends with MA
- [ ] Page 2: Weekday comparison
- [ ] Page 2: Calendar heatmap
- [ ] Page 3: Hour x Day heatmap
- [ ] Page 3: Peak hours bars
- [ ] Page 3: Speed by hour
- [ ] Page 4: Borough treemap
- [ ] Page 4: Flow pie chart
- [ ] Page 4: Top routes bars
- [ ] Page 4: Efficiency scatter
- [ ] Filters added (Date, Borough)
- [ ] Actions configured (Highlight, Filter)
- [ ] Consistent colors and fonts
- [ ] Informative tooltips
- [ ] Titles and descriptions
- [ ] Tested on Desktop/Tablet/Mobile

## Business Questions Examples

Your dashboard should be able to answer:

1. **"Which month generated the most revenue?"**
   → Page 1, Monthly Revenue Chart

2. **"Is there growth or decline?"**
   → Page 1, MoM Growth Bars

3. **"What are the busiest hours?"**
   → Page 3, Hour Heatmap + Peak Hours

4. **"Weekend vs Weekday: what's the difference?"**
   → Page 2, Weekday Comparison

5. **"Which borough generates the most revenue?"**
   → Page 4, Borough Treemap

6. **"What are the most popular routes?"**
   → Page 4, Top Routes Bars

7. **"What is the average speed by hour?"**
   → Page 3, Speed vs Hour Line

## Next Steps

After creating the basic dashboard:

1. **Add predictions** (30-day Forecast)
2. **Create alerts** (If MoM < -10%)
3. **Add segments** (Customer types if available)
4. **Geo-mapping** (If coordinates available)
5. **Real-time refresh** (If pipeline scheduled)

Your dashboard is now ready to impress recruiters!