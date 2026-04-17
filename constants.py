# Shared application-wide constants

# Earliest year for which market data is available
MIN_MARKET_DATA_YEAR = 1994

# Trading calendar
TRADING_DAYS_PER_YEAR = 252

# Chart bar widths in nanoseconds for Plotly timestamp-axis bar charts.
# Legacy COT reports have 2 participant groups; disaggregated/TFF have 3,
# so the narrower width keeps bars from overlapping.
BAR_WIDTH_LEGACY = 70_000_000
BAR_WIDTH_DISAGGREGATED = 60_000_000

# OHLC record count thresholds that determine the default viewport end date.
# < Q1_THRESHOLD → show through Q1; < Q2_THRESHOLD → show through Q2.
OHLC_Q1_THRESHOLD = 30
OHLC_Q2_THRESHOLD = 60

# Buffer padding (days) added to each side of the initial x-axis range
RANGE_BUFFER_DAYS = 2

# Hover crosshair detection radius in pixels
HOVER_DISTANCE_PX = 100

# Number of previous years to fall back to when the target year has no data
MAX_YEAR_FALLBACK_ATTEMPTS = 3

# Tooltip show-delay in milliseconds used in layout components
TOOLTIP_DELAY_MS = 300

# Number of clusters used in KMeans distribution analysis
KMEANS_CLUSTERS = 3

# Maximum calendar-day delta when searching for a nearest available date
MAX_DATE_DELTA_DAYS = 3

# Multiplier to convert a ratio to a percentage (0.05 → 5 %)
PCT_SCALE_FACTOR = 100
