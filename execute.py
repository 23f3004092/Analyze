import json
import pandas as pd

def main():
    # Read the data
    # df = pd.read_excel("data.xlsx")
    df = pd.read_csv("data.csv") # Changed to read CSV for compatibility

    # Compute revenue
    df["revenue"] = df["units"] * df["price"]

    # row_count
    row_count = len(df)

    # regions: count of distinct regions
    regions_count = df["region"].nunique()

    # top_n_products_by_revenue (n=3)
    n = 3
    top_products = (
        df.groupby("product")["revenue"]
        .sum()
        .sort_values(ascending=False)
        .head(n)
        .reset_index()
    )
    top_products_list = [
        {"product": row["product"], "revenue": float(row["revenue"]) } 
        for _, row in top_products.iterrows()
    ]

    # rolling_7d_revenue_by_region: for each region, last value of 7-day moving average of daily revenue
    df["date"] = pd.to_datetime(df["date"])
    daily_rev = (
        df.groupby(["region", "date"])["revenue"]  # daily revenue per region - corrected typo
        .sum()
        .reset_index(name="daily_revenue") # Added name for clarity
    )

    # Compute 7-day rolling mean of revenue per region, retaining the region column
    # Ensuring 'daily_rev' is sorted by region and date before applying rolling group
    daily_rev_sorted = daily_rev.sort_values(by=['region', 'date'])
    rolling = (
        daily_rev_sorted.groupby("region")
        .apply(lambda g: g.set_index("date")["daily_revenue"].rolling("7D").mean(), include_groups=False)
        .reset_index(name="rolling_7d_revenue")
    )

    # Get the last value of the rolling average for each region
    last_rolling = (
        rolling.sort_values(["region", "date"])  # ensure order
        .groupby("region")
        .tail(1)
    )

    rolling_summary = {
        row["region"]: float(row["rolling_7d_revenue"]) if pd.notna(row["rolling_7d_revenue"]) else 0.0 
        for _, row in last_rolling.iterrows()
    }

    result = {
        "row_count": int(row_count),
        "regions": int(regions_count),
        "top_n_products_by_revenue": top_products_list,
        "rolling_7d_revenue_by_region": rolling_summary,
    }

    # Output result to result.json (which will be published by CI)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
