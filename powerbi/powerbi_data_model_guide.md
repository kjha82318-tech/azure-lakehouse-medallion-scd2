# Power BI Data Model & DAX Guide

This guide details the Star Schema data model and DAX measures for visual reporting in Power BI Desktop or the Power BI Service.

---

## 1. Star Schema Relationship Architecture

```
       ┌────────────────────────┐
       │     dim_customers      │ (SCD Type 2)
       ├────────────────────────┤
       │ PK  customer_surr_key  │
       │     customer_id        │
       │     customer_name      │
       │     loyalty_tier       │
       │     city, state        │
       │     start_date         │
       │     end_date           │
       │     is_current         │
       └───────────┬────────────┘
                   │ 1
                   │
                   │ *
       ┌───────────┴────────────┐
       │      fact_orders       │
       ├────────────────────────┤
       │ PK  order_id           │
       │ FK  customer_id        │
       │ FK  customer_surr_key  │
       │     product_name       │
       │     category           │
       │     unit_price         │
       │     quantity           │
       │     total_amount       │
       │     order_date         │
       └────────────────────────┘
```

- **Relationship:** 1-to-Many (`1:*`) from `dim_customers[customer_surrogate_key]` to `fact_orders[customer_surrogate_key]`.
- **Cross Filter Direction:** Single.

---

## 2. Core Business DAX Measures

### Total Revenue
```dax
Total Revenue = SUM(fact_orders[total_amount])
```

### Total Order Count
```dax
Total Orders = DISTINCTCOUNT(fact_orders[order_id])
```

### Average Order Value (AOV)
```dax
Average Order Value = 
DIVIDE([Total Revenue], [Total Orders], 0)
```

### Currently Active Customer Count
```dax
Active Customers = 
CALCULATE(
    DISTINCTCOUNT(dim_customers[customer_id]),
    dim_customers[is_current] = TRUE()
)
```

### Revenue Contribution by Loyalty Tier (%)
```dax
% Tier Revenue = 
DIVIDE(
    [Total Revenue],
    CALCULATE([Total Revenue], ALL(dim_customers[loyalty_tier])),
    0
)
```

---

## 3. How to Connect Power BI to the Project Output

1. Run the master pipeline: `python -m src.run_pipeline`.
2. Open **Power BI Desktop**.
3. Click **Get Data** ➔ **Text/CSV**.
4. Select `data/powerbi_export/dim_customers.csv` and `data/powerbi_export/fact_orders.csv`.
5. Switch to **Model View** and verify the relationship between `customer_surrogate_key`.
6. Add visuals:
   - **Card:** Total Revenue, Total Orders, Average Order Value.
   - **Bar Chart:** Revenue by Product Category.
   - **Donut Chart:** Orders by Loyalty Tier (Gold vs. Silver vs. Bronze).
   - **Map / Matrix:** Revenue by City and State.
