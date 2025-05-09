# Item Migration Verification Tool

This tool verifies the data migration between old and new item master tables across multiple tenant databases.

## Features

- Compares attribute values between old and new flat tables
- Verifies common column values between tables
- Supports multiple tenant databases in a single run
- Detailed mismatch reporting with colorized output

## Setup

1. Install dependencies:
```bash
npm install
```

2. Create a `.env` file based on `.env.example` with your database credentials:
```
DB_HOST=localhost
DB_USER=your_username
DB_PASSWORD=your_password
DB_PORT=3306
TENANT_DBS=tenant1,tenant2,tenant3  # Comma-separated list of tenant database names
```

## Running the Tool

```bash
npm start
```

## What it Verifies

1. **Attribute Mappings**:
   - Maps old attributes (`a_<name>`) to new attributes (`a<flat_table_index>`)
   - Compares values between corresponding columns

2. **Common Columns**:
   - Verifies values of common columns between old and new tables
   - Includes: ref_sku_code, name, descriptions, UOM details, etc.

## Output

The tool will:
- Show progress for each tenant database
- Report any mismatches found
- Display detailed comparison results for any differences
- Use color coding for better visibility:
  - 🟢 Green: All matches
  - 🟡 Yellow: Warnings
  - 🔴 Red: Mismatches/Errors 