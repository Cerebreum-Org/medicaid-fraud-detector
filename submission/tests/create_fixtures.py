"""
Generate synthetic test fixtures for all 6 fraud signals.

Creates small parquet files in tests/fixtures/ that trigger each signal
when the detection engine runs against them.
"""

import os
import duckdb

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
os.makedirs(FIXTURES_DIR, exist_ok=True)

con = duckdb.connect()

# === Build NPPES rows ===
nppes_rows = []

# 100 normal CA peers for taxonomy 207Q00000X (needed for Signal 2 p99)
for i in range(1, 101):
    npi = f"2{i:09d}"
    nppes_rows.append(
        f"('{npi}', '1', NULL, 'PEER{i}', 'P{i}', 'CA', '9{i:04d}', '207Q00000X', '01/15/2019', NULL, NULL)"
    )

# Special signal providers
special = [
    # Signal 1: Excluded provider
    "('1000000099', '1', NULL, 'EXCLUDED', 'PERSON', 'TX', '75001', '207Q00000X', '06/01/2018', NULL, NULL)",
    # Signal 1 Tier 2: Name-matched excluded (no NPI in LEIE)
    "('1000000098', '1', NULL, 'BADACTOR', 'FRANK', 'NY', '10001', '207Q00000X', '03/15/2017', NULL, NULL)",
    # Signal 2: Volume outlier — same taxonomy/state as CA peers
    "('1000000050', '1', NULL, 'BIGBILLER', 'MAX', 'CA', '90220', '207Q00000X', '01/15/2018', NULL, NULL)",
    # Signal 3: Rapid escalation — recently enumerated
    "('1000000060', '1', NULL, 'NEWGUY', 'FAST', 'CA', '90221', '207Q00000X', '09/01/2024', NULL, NULL)",
    # Signal 4: Workforce impossibility — organization
    "('1000000070', '2', 'MEGACORP HEALTH LLC', 'MEGA', 'CORP', 'FL', '33101', '251E00000X', '01/01/2015', 'SHADY', 'BOSS')",
    # Signal 5: Shared authorized official — 6 orgs
    "('1000000080', '2', 'SHELL CORP A', NULL, NULL, 'FL', '33102', '251E00000X', '01/01/2018', 'KINGPIN', 'CARL')",
    "('1000000081', '2', 'SHELL CORP B', NULL, NULL, 'FL', '33103', '251E00000X', '02/01/2018', 'KINGPIN', 'CARL')",
    "('1000000082', '2', 'SHELL CORP C', NULL, NULL, 'FL', '33104', '251E00000X', '03/01/2018', 'KINGPIN', 'CARL')",
    "('1000000083', '2', 'SHELL CORP D', NULL, NULL, 'TX', '75002', '251E00000X', '04/01/2018', 'KINGPIN', 'CARL')",
    "('1000000084', '2', 'SHELL CORP E', NULL, NULL, 'TX', '75003', '251E00000X', '05/01/2018', 'KINGPIN', 'CARL')",
    "('1000000085', '2', 'SHELL CORP F', NULL, NULL, 'NY', '10002', '251E00000X', '06/01/2018', 'KINGPIN', 'CARL')",
    # Signal 6: Geographic implausibility
    "('1000000090', '1', NULL, 'PHANTOM', 'HOME', 'GA', '30301', '251E00000X', '01/01/2016', NULL, NULL)",
]
nppes_rows.extend(special)

nppes_values = ",\n            ".join(nppes_rows)
con.execute(f"""
    COPY (
        SELECT * FROM (VALUES
            {nppes_values}
        ) AS t(npi, entity_type, org_name, last_name, first_name, state, postal_code, taxonomy, enumeration_date, auth_off_last, auth_off_first)
    ) TO '{FIXTURES_DIR}/nppes_slim.parquet' (FORMAT PARQUET);
""")

# === LEIE fixture ===
con.execute(f"""
    COPY (
        SELECT * FROM (VALUES
            ('EXCLUDED', 'PERSON', NULL, '1000000099', '1128a1', '20200101', NULL, 'TX', 'GENERAL PRACTICE'),
            ('BADACTOR', 'FRANK', NULL, NULL, '1128a1', '20190601', NULL, 'NY', 'GENERAL PRACTICE'),
            ('UNRELATED', 'SOMEONE', NULL, '9999999999', '1128a1', '20180101', NULL, 'CA', 'DENTIST')
        ) AS t(LASTNAME, FIRSTNAME, MIDNAME, NPI, EXCLTYPE, EXCLDATE, REINDATE, STATE, SPECIALTY)
    ) TO '{FIXTURES_DIR}/leie.parquet' (FORMAT PARQUET);
""")

# === Spending fixture ===
rows = []

# 100 normal CA peers — modest billing ($5k/month for 6 months = $30k each)
for i in range(1, 101):
    npi = f"2{i:09d}"
    for month_offset in range(6):
        month = f"2024-{month_offset + 1:02d}-01"
        rows.append((npi, npi, month, 'G0101', 50, 5000.0, 30, 'CA'))

# Signal 1: Excluded provider billing after exclusion date (2020-01-01)
for month_offset in range(6):
    month = f"2024-{month_offset + 1:02d}-01"
    rows.append(('1000000099', '1000000099', month, '99213', 100, 15000.0, 50, 'TX'))

# Signal 1 Tier 2: Name-matched excluded provider
for month_offset in range(6):
    month = f"2024-{month_offset + 1:02d}-01"
    rows.append(('1000000098', '1000000098', month, '99213', 80, 12000.0, 40, 'NY'))

# Signal 2: Volume outlier — same taxonomy/state but 100x billing ($500k/month)
for month_offset in range(6):
    month = f"2024-{month_offset + 1:02d}-01"
    rows.append(('1000000050', '1000000050', month, '99213', 5000, 500000.0, 200, 'CA'))

# Signal 3: Rapid escalation — new provider with explosive growth
# Enumerated 09/01/2024, billing starts 10/2024
growth_amounts = [100, 200, 500, 1500, 5000, 15000, 50000, 150000, 500000, 500000, 500000, 500000]
for i, amount in enumerate(growth_amounts):
    month_num = ((10 + i - 1) % 12) + 1
    year = 2024 if month_num >= 10 else 2025
    month = f"{year}-{month_num:02d}-01"
    claims = max(10, amount // 10)
    rows.append(('1000000060', '1000000060', month, '99213', claims, float(amount), 20, 'CA'))

# Signal 4: Workforce impossibility — org with 2000 claims in a single month
rows.append(('1000000070', '1000000070', '2024-06', '99213', 2000, 200000.0, 100, 'FL'))
rows.append(('1000000070', '1000000070', '2024-07', '99213', 500, 50000.0, 50, 'FL'))

# Signal 5: Shared authorized official — 6 shell corps each billing $80k/month
for npi_suffix in range(80, 86):
    for month_offset in range(3):
        month = f"2024-{month_offset + 1:02d}-01"
        rows.append((f'10000000{npi_suffix}', f'10000000{npi_suffix}', month, '99213', 200, 80000.0, 50, 'FL'))

# Signal 6: Geographic implausibility — home health with 0.01 beneficiary/claim ratio
for month_offset in range(6):
    month = f"2024-{month_offset + 1:02d}-01"
    rows.append(('1000000090', '1000000090', month, 'T1019', 500, 50000.0, 5, 'GA'))

# Build the spending parquet (7 columns matching real schema)
values_str = ",\n            ".join(
    f"('{r[0]}', '{r[1]}', '{r[2]}', '{r[3]}', {r[4]}, {r[5]}, {r[6]})"
    for r in rows
)

con.execute(f"""
    COPY (
        SELECT * FROM (VALUES
            {values_str}
        ) AS t(BILLING_PROVIDER_NPI_NUM, SERVICING_PROVIDER_NPI_NUM, CLAIM_FROM_MONTH, HCPCS_CODE, TOTAL_CLAIMS, TOTAL_PAID, TOTAL_UNIQUE_BENEFICIARIES)
    ) TO '{FIXTURES_DIR}/spending.parquet' (FORMAT PARQUET);
""")

con.close()
print(f"Fixtures created in {FIXTURES_DIR}")
print(f"  nppes_slim.parquet")
print(f"  leie.parquet")
print(f"  spending.parquet")
print(f"  Total spending rows: {len(rows)}")
