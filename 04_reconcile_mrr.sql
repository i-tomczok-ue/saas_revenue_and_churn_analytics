-- Porównanie Gold z wzorcowym MRR wyliczonym przez generator (ground_truth_mrr.csv).
-- Uruchom w edytorze SQL. Kolumna diff_* powinna być 0 dla każdego miesiąca, który jest już załadowany.
WITH gt(year_month, mrr, new_mrr, expansion_mrr, contraction_mrr, churned_mrr) AS (
  VALUES
    ('2025-01', 1840.0, 1840.0, 0.0, 0.0, 0.0),
    ('2025-02', 6873.0, 5033.0, 0.0, 0.0, 0.0),
    ('2025-03', 7474.0, 943.0, 56.0, 0.0, 398.0),
    ('2025-04', 12912.0, 5076.0, 610.0, 0.0, 248.0),
    ('2025-05', 20093.0, 7279.0, 0.0, 0.0, 98.0),
    ('2025-06', 24870.0, 3882.0, 1200.0, 8.0, 297.0),
    ('2025-07', 28284.0, 3192.0, 336.0, 16.0, 98.0),
    ('2025-08', 30070.0, 2483.0, 500.0, 0.0, 1197.0),
    ('2025-09', 33903.0, 2584.0, 2036.0, 490.0, 297.0),
    ('2025-10', 35339.0, 2976.0, 90.0, 120.0, 1510.0),
    ('2025-11', 37338.0, 1589.0, 638.0, 32.0, 196.0),
    ('2025-12', 39912.0, 3541.0, 928.0, 600.0, 1295.0),
    ('2026-01', 45271.0, 3344.0, 2740.0, 240.0, 485.0),
    ('2026-02', 49139.0, 4642.0, 1340.0, 1000.0, 1114.0),
    ('2026-03', 52334.0, 2941.0, 2660.0, 534.0, 1872.0),
    ('2026-04', 57960.0, 4874.0, 2472.0, 150.0, 1570.0),
    ('2026-05', 59539.0, 3532.0, 504.0, 256.0, 2201.0),
    ('2026-06', 64774.0, 6330.0, 1128.0, 20.0, 2203.0),
    ('2026-07', 66823.0, 2144.0, 1578.0, 126.0, 1547.0),
    ('2026-08', 73697.0, 7767.0, 828.0, 8.0, 1713.0),
    ('2026-09', 78912.0, 5069.0, 2932.0, 8.0, 2778.0),
    ('2026-10', 81506.0, 3432.0, 2210.0, 1280.0, 1768.0),
    ('2026-11', 88230.0, 7478.0, 1454.0, 726.0, 1482.0),
    ('2026-12', 90319.0, 4248.0, 1998.0, 248.0, 3909.0)
),
gold AS (
  SELECT year_month,
         SUM(mrr) AS mrr, SUM(new_mrr) AS new_mrr, SUM(expansion_mrr) AS expansion_mrr,
         SUM(contraction_mrr) AS contraction_mrr, SUM(churned_mrr) AS churned_mrr
  FROM workspace.saas.fact_subscription_monthly
  GROUP BY year_month
)
SELECT g.year_month,
       g.mrr, t.mrr AS mrr_wzorcowy,
       ROUND(g.mrr - t.mrr, 2)                       AS diff_mrr,
       ROUND(g.new_mrr - t.new_mrr, 2)               AS diff_new,
       ROUND(g.expansion_mrr - t.expansion_mrr, 2)   AS diff_expansion,
       ROUND(g.contraction_mrr - t.contraction_mrr, 2) AS diff_contraction,
       ROUND(g.churned_mrr - t.churned_mrr, 2)       AS diff_churned
FROM gold g JOIN gt t USING (year_month)
ORDER BY g.year_month;
