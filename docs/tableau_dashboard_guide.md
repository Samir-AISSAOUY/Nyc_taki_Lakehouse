# Tableau Public-NYC Taxi Dashboard (In building) 

## Data Preparation

### Step 1: Copy CSV Files from Docker

1. Open a terminal or command prompt
2. Create a folder for Tableau data:
   ```bash
   mkdir tableau_data
   ```
3. Copy all CSV files from the Docker container:
   ```bash
   docker cp spark-master:/data/tableau_exports/. tableau_data/
   ```
4. Verify all files were correctly copied:
   ```bash
   ls tableau_data
   ```

### Step 2: Install Tableau Desktop

1. Download Tableau Desktop from: https://www.tableau.com/products/desktop/download
2. Install the software following the instructions
3. For students, a free license is available through Tableau's academic program

## Importing the Data

### Step 1: Launch Tableau Desktop and Create a New Connection

1. Launch Tableau Desktop
2. On the home screen, in the "Connect" panel, click "Text File"

### Step 2: Import CSV Files (in order)

1. **Import kpi_monthly.csv**:
   - Navigate to `tableau_data/kpi_monthly`
   - Select the CSV file (usually part-00000-*.csv)
   - Rename the data source to "KPI Monthly" in the left panel
   - Click on "Sheet 1" at the bottom to continue

2. **Import daily_trends.csv**:
   - Click "Data" in the top menu
   - Select "New Data Source" > "Text File"
   - Navigate to `tableau_data/daily_trends`
   - Select the CSV file
   - Rename the source to "Daily Trends"

3. **Repeat for other key files**:
   - hourly_heatmap.csv → "Hourly Heatmap"
   - borough_summary.csv → "Borough Summary"
   - top_routes_volume.csv → "Top Routes"
   - performance_ranking.csv → "Performance Ranking"
   - weekday_comparison.csv → "Weekday Comparison"
   - peak_hours.csv → "Peak Hours"
   - route_efficiency.csv → "Route Efficiency"
   - borough_flow.csv → "Borough Flow"

### Step 3: Configure Relationships Between Data Sources

1. Click "Data" > "Edit Relationships"
2. Create the following relationships:
   - Between "KPI Monthly" and "Daily Trends": 
     - Drag "KPI Monthly" to "Daily Trends"
     - Choose relationship "year_month" in KPI Monthly → "pickup_date" (converted to year-month) in Daily Trends
   - Between "Daily Trends" and "Hourly Heatmap":
     - Drag "Daily Trends" to "Hourly Heatmap"
     - Link on "pickup_date"
   - Between "Borough Summary" and "Top Routes":
     - Drag "Borough Summary" to "Top Routes"
     - Link "PU_Borough" on both sides
3. Click "OK" to confirm relationships

## Creating the Dashboard

### Step 1: Dashboard Configuration

1. Create a new dashboard:
   - Click the "New Dashboard" icon at the bottom
2. Configure the size:
   - In the left panel, under "Size", select "Fixed"
   - Set the size to 1920 x 1080 pixels (desktop format)
3. Create a layout for each device:
   - Click "Dashboard" > "New Device Layout"
   - Create a tablet version (1024 x 768)
   - Create a mobile version (768 x 1024)

### Step 2: Creating Page 1 - OVERVIEW (KPI Executive)

#### A. Create the 4 KPI Cards

1. **Create the "Total Trips YTD" sheet**:
   - Create a new sheet (+ icon at bottom)
   - Rename it to "Total Trips YTD"
   - Use the "KPI Monthly" source
   - Drag "total_trips_ytd" to the "Text" area in the center
   - Formatting:
     - Right-click on the number > "Format..."
     - In "Numbers", choose "Custom" and enter "#,##0"
     - In "Color", choose a blue gradient
     - Increase font size to 48pt

2. **Create the "Total Revenue YTD" sheet**:
   - New sheet, rename it
   - Drag "total_revenue_ytd" to the "Text" area
   - Formatting:
     - Format "Currency" → "$#,##0"
     - Color: green gradient
     - Size: 48pt
     - Prefix: $

3. **Create the "Average Fare" sheet**:
   - New sheet, rename it
   - Drag "avg_fare_ytd" to the "Text" area
   - Formatting:
     - Format "Currency" → "$0.00"
     - Color: orange
     - Size: 48pt

4. **Create the "Avg Distance" sheet**:
   - New sheet, rename it
   - Drag "avg_distance_ytd" to the "Text" area
   - Formatting:
     - Format "Number" → "0.00"
     - Add suffix "miles"
     - Color: purple
     - Size: 48pt

#### B. Create the Monthly Revenue Trend Chart

1. Create a new sheet named "Monthly Revenue Trend"
2. Use the "KPI Monthly" source
3. Drag "year_month" to Columns
4. Drag "monthly_revenue" to Rows
5. In the "Marks" menu, select "Line"
6. Add circles to the line:
   - In "Marks", click the dropdown and add a "Circle"
7. Color the line in green gradient:
   - Click "Color" and choose a green gradient
8. Add a trend line:
   - Right-click on the chart
   - Select "Trend Line"
   - Choose "Linear"
9. Add a reference line for the average:
   - Right-click on the Y-axis
   - Select "Add Reference Line"
   - Under "Line", choose "Average"
   - Label it "Average Revenue"

#### C. Create the MoM Growth Chart

1. Create a new sheet named "MoM Growth"
2. Use the "KPI Monthly" source
3. Drag "year_month" to Columns
4. Drag "mom_growth_pct" to Rows
5. In "Marks", select "Bar"
6. Define conditional coloring:
   - Click "Color"
   - Select "Edit Colors"
   - Choose "By Field"
   - Select "mom_growth_pct"
   - Set colors: green if > 0, red if < 0
7. Add a zero reference line:
   - Right-click on the Y-axis
   - Select "Add Reference Line"
   - Value: 0
   - Label: "No Growth"

#### D. Create the Best/Worst Days Table

1. Create a new sheet named "Best Worst Days"
2. Use the "Performance Ranking" source
3. Drag "pickup_date" and "rank_type" to Rows
4. Drag "total_trips" and "total_revenue" to Columns
5. For coloring:
   - Drag "rank_type" to "Color"
   - Customize: "Top 10 Days" in green, "Bottom 10 Days" in red
6. Sort by revenue:
   - Click the "total_revenue" column header
   - Select "Sort Descending"

#### E. Assemble the "Overview" Dashboard

1. Return to the previously created dashboard
2. Drag objects into the layout:
   - In the top row, place the 4 KPI cards side by side
   - In the middle section, place the "Monthly Revenue Trend" chart
   - In the bottom section, place "MoM Growth" on the left and "Best Worst Days" on the right
3. Adjust sizes and spacing for a balanced layout
4. Add a title: "NYC Taxi Executive Dashboard"
5. Add subtitles for each section

### Step 3: Creating Page 2 - TRENDS (Time Series)

#### A. Create the Daily Trips with Moving Averages Chart

1. Create a new sheet named "Daily Trips with MA"
2. Use the "Daily Trends" source
3. Drag "pickup_date" to Columns
4. Drag "total_trips" to Rows
5. Duplicate the Y-axis for moving averages:
   - Right-click on "total_trips" in Rows
   - Select "Duplicate"
   - Do this again to get a third axis
6. Configure each measure:
   - First axis: Rename to "Daily Trips", blue color, solid line
   - Second axis: Replace with "trips_ma7", rename to "7-Day MA", orange color, dashed line
   - Third axis: Replace with "trips_ma30", rename to "30-Day MA", red color, dashed line
7. Add a forecast:
   - Right-click on the X-axis
   - Select "Forecast"
   - Set duration to 30 days

#### B. Create the Weekday vs Weekend Comparison

1. Create a new sheet named "Weekday vs Weekend"
2. Use the "Weekday Comparison" source
3. Drag "is_weekend" to Columns
4. Drag "avg_daily_trips" to Rows
5. Add "avg_daily_revenue" as a second Y-axis:
   - Drag "avg_daily_revenue" to the Y-axis
   - Right-click on the axis → "Dual Axis"
6. In "Marks", set the type to "Bar"
7. For coloring:
   - Drag "is_weekend" to "Color"
   - Customize: "Weekend" in orange, "Weekday" in blue

#### C. Create the Daily Revenue Calendar (heatmap)

1. Create a new sheet named "Daily Revenue Calendar"
2. Use the "Daily Trends" source
3. Drag "pickup_date" to Columns
4. Click the calendar icon in "pickup_date" and select "Week"
5. Drag "pickup_date" again to Rows
6. Click the calendar icon and select "Weekday"
7. Drag "total_revenue" to "Color"
8. In "Marks", select "Square"
9. Customize the color gradient:
   - Blue-Red gradient
   - Adjust the scale for good visualization

#### D. Assemble the "Trends" Dashboard

1. Create a new dashboard:
   - Click the "New Dashboard" icon at the bottom
   - Configure the same size as the previous one
2. Arrange the elements:
   - Place "Daily Trips with MA" at the top, take 2/3 of the height
   - Place "Weekday vs Weekend" and "Daily Revenue Calendar" side by side at the bottom
3. Add a global date filter:
   - Click on one of the charts
   - Click "Filter" in the menu
   - Select "pickup_date"
   - Choose "All dashboard"
4. Add a title and explanatory annotations

### Step 4: Creating Page 3 - HEATMAP (Hourly Patterns)

#### A. Create the Hour x Day Heatmap

1. Create a new sheet named "Hour x Day Heatmap"
2. Use the "Hourly Heatmap" source
3. Drag "pickup_hour" to Columns
4. Drag "day_name" to Rows
5. Ensure the days are in the correct order:
   - Right-click on the "day_name" axis
   - Select "Edit Order..."
   - Arrange in this order: Sunday, Monday, Tuesday, etc.
6. Drag "avg_trips" to "Color"
7. Also drag "avg_trips" to "Size"
8. In "Marks", select "Square"
9. Customize the color gradient:
   - Choose a Yellow-Red gradient to show intensity
10. Configure the tooltip:
    - Click "Tooltip"
    - Add "avg_fare" and "avg_speed"
    - Format for good readability

#### B. Create the Peak Hours Chart

1. Create a new sheet named "Peak Hours"
2. Use the "Peak Hours" source
3. Drag "pickup_hour" to Columns
4. Drag "total_trips_by_hour" to Rows
5. In "Marks", select "Bar"
6. Set a gradient coloring:
   - Click "Color"
   - Select a light blue to dark blue gradient
7. Highlight the top 3 hours:
   - Create a set for the top 3 peak hours
   - Use this set to highlight those bars

#### C. Create the Speed vs Hour Chart

1. Create a new sheet named "Speed vs Hour"
2. Use the "Hourly Heatmap" source
3. Drag "pickup_hour" to Columns
4. Drag "avg_speed" to Rows
5. In "Marks", select "Line"
6. Add an area under the line:
   - In the "Marks" menu, click "Add Mark"
   - Select "Area"
7. Set a color by speed:
   - Click "Color"
   - Choose a gradient based on speed
8. Add a reference line for speed limit:
   - Right-click on the Y-axis
   - Select "Add Reference Line"
   - Value: 25 (typical NYC speed limit)
   - Label: "City Speed Limit"

#### D. Assemble the "Heatmap" Dashboard

1. Create a new dashboard
2. Arrange the elements:
   - Place "Hour x Day Heatmap" at the top, take 1/2 of the height
   - Place "Peak Hours" and "Speed vs Hour" side by side at the bottom
3. Add a global hour filter:
   - Drag "pickup_hour" from one of the charts
   - Configure as a filter for the entire dashboard
   - Choose "Slider" as display style
4. Add a weekday filter:
   - Drag "day_name" from "Hour x Day Heatmap"
   - Configure as a filter for the entire dashboard
   - Choose "Multiple Values (list)" as display style

### Step 5: Creating Page 4 - GEOGRAPHY (Borough & Routes)

#### A. Create the Borough Performance Treemap

1. Create a new sheet named "Borough Performance"
2. Use the "Borough Summary" source
3. In "Marks", select "Treemap"
4. Drag "PU_Borough" to "Detail"
5. Drag "total_revenue" to "Size"
6. Drag "avg_fare" to "Color"
7. Configure labels:
   - Drag "PU_Borough" to "Label"
   - Also add "total_revenue" (formatted)
8. Configure the tooltip:
   - Add all available metrics
   - Format each metric clearly

#### B. Create the Inter vs Intra Borough Pie Chart

1. Create a new sheet named "Borough Flow"
2. Use the "Borough Flow" source
3. In "Marks", select "Pie"
4. Drag "flow_type" to "Color"
5. Drag "total_trips" to "Angle"
6. Customize colors:
   - "Intra-Borough": Blue
   - "Inter-Borough": Orange
7. Configure labels:
   - Drag "flow_type" to "Label"
   - Also add a percentage

#### C. Create the Top 20 Routes Chart

1. Create a new sheet named "Top 20 Routes"
2. Use the "Top Routes" source
3. Create a calculated field for routes:
   - Right-click in the data space > "Create" > "Calculated Field..."
   - Name: "Route"
   - Formula: `[PU_Zone] + " → " + [DO_Zone]`
4. Drag "Route" to Rows
5. Drag "total_trips" to Columns
6. In "Marks", select "Bar"
7. Sort by trip count:
   - Right-click on the X-axis
   - Select "Sort"
   - Choose "By Field" > "total_trips" > "Descending"
8. Limit to 20 routes:
   - Right-click on "Route"
   - Select "Filter"
   - Choose "Top" > "By Field" > 20 > "total_trips"
9. Color by total revenue:
   - Create a calculated field: "total_revenue_route" = `[total_trips] * [avg_fare]`
   - Drag this field to "Color"
   - Choose an appropriate gradient

#### D. Create the Route Efficiency Scatter Plot

1. Create a new sheet named "Route Efficiency"
2. Use the "Route Efficiency" source
3. Drag "avg_distance" to Columns
4. Drag "revenue_per_mile" to Rows
5. In "Marks", select "Circle"
6. Drag "total_trips" to "Size"
7. Drag "PU_Borough" to "Color"
8. Configure the tooltip:
   - Include route details
   - Add all relevant metrics

#### E. Assemble the "Geography" Dashboard

1. Create a new dashboard
2. Arrange the elements:
   - Top left: "Borough Performance" (1/2 width, 1/2 height)
   - Top right: "Borough Flow" (1/2 width, 1/2 height)
   - Bottom left: "Top 20 Routes" (2/3 width, 1/2 height)
   - Bottom right: "Route Efficiency" (1/3 width, 1/2 height)
3. Add a global Borough filter:
   - Drag "PU_Borough" from "Borough Performance"
   - Configure as a filter for all elements
4. Configure interaction actions:
   - Click "Dashboard" > "Actions"
   - Create a "Highlight" action:
     - Source: "Borough Performance"
     - Target: "Top 20 Routes"
     - Run On: Select
   - Create a URL action:
     - Source: "Borough Performance"
     - URL: `https://www.google.com/maps/search/<PU_Borough>+NYC`
     - Run On: Select

### Step 6: Finalizing the Dashboard

#### A. Creating the Main Dashboard

1. Create a tabbed dashboard:
   - Click "Dashboard" > "New Dashboard Tab"
2. Add all four created dashboards as tabs:
   - Page 1: OVERVIEW
   - Page 2: TRENDS
   - Page 3: HEATMAP
   - Page 4: GEOGRAPHY
3. Format the appearance of tabs:
   - Consistent background color
   - Uniform fonts
   - Representative icons

#### B. Apply the Design Style

1. Apply the color palette:
   - Primary Blue: #1f77b4
   - Success Green: #2ca02c
   - Warning Orange: #ff7f0e
   - Danger Red: #d62728
   - Purple: #9467bd
2. Apply the fonts:
   - Title: Tableau Bold, 24pt
   - Subtitle: Tableau Regular, 16pt
   - Body: Tableau Book, 12pt
   - Numbers: Tableau Medium, 14pt
3. Ensure consistent alignment and spacing:
   - Use a 16px grid
   - Align all elements properly

#### C. Configure Responsive Design

1. For each device layout (desktop, tablet, mobile):
   - Adapt content to screen size
   - For tablet: Reduce to 2 columns
   - For mobile: Stack in 1 column, keep only key metrics

#### D. Perform Comprehensive Testing

1. Test all interactions:
   - Verify all filters work
   - Verify all interaction actions work
   - Ensure tooltips are informative
2. Test on different screen sizes
3. Check consistency of colors and fonts

### Step 7: Publishing the Dashboard

#### A. Preparation for Publication

1. Check all items in the checklist:
   - Complete all points in the provided checklist
   - Ensure all visualizations are created and functional

#### B. Publishing to Tableau Public

1. Create a Tableau Public account if you don't have one
2. Save to Tableau Public:
   - File > Save to Tableau Public
   - Log in to your account
   - Give a descriptive title
   - Add a detailed description
   - Add relevant tags
3. Configure visibility:
   - Public (recommended for portfolios)
   - Share the link

#### C. Alternative Exports

1. Export as PDF:
   - File > Print to PDF
   - Include all pages
   - Choose high resolution
2. Export as images:
   - Dashboard > Export Image
   - Resolution: 300 DPI
   - Format: PNG

## Final Verification

Go through this checklist to ensure you've completed all steps:

- [ ] CSV data import
- [ ] Relationships between sources configuration
- [ ] Page 1: KPI cards (4 metrics)
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
- [ ] Global filters added
- [ ] Interaction actions configured
- [ ] Consistent colors and fonts
- [ ] Informative tooltips
- [ ] Titles and descriptions added
- [ ] Tested on Desktop/Tablet/Mobile
- [ ] Publication

## Advanced Tips

To take your dashboard further:

1. **Add forecasts**: Use the forecasting feature for 30-day projections
2. **Create alerts**: Configure alerts (if MoM < -10%)
3. **Add segments** if you have customer type data
4. **Explore geo-mapping**: If you have precise coordinates, create interactive maps
5. **Set up automatic refresh** if your pipeline is scheduled

Let's talk to data now ^^
