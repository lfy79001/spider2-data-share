# Spider 2 Migration Scripts

A collection of scripts for migrating Spider 2 database data between Snowflake accounts with configurable connection settings.

## 1. Installation

Create python environment

```bash
conda create -n "spider2-migration" python=3.10.13
conda activate spider2-migration
pip install -r requirements.txt
```

## 2. Configuration

⚠️ **Configuration is required!** The scripts support flexible configuration through environment variables or command line arguments.

### Environment Variables (Recommended)

Set the following **required** environment variables for your Snowflake configuration:

```bash
export SNOWFLAKE_ACCOUNT=your_account_identifier
export SNOWFLAKE_ROLE=your_default_role
export SNOWFLAKE_WAREHOUSE=your_warehouse
export SNOWFLAKE_ADMIN_ROLE=your_admin_role
export SNOWFLAKE_READONLY_ROLE=your_readonly_role
export DESTINATION_USER=your_username
```

All of these environment variables are required. If any are missing, the scripts will display an error message indicating which configuration values need to be provided.

## 3. Merge databases

Run script below using source account (from cortex root folder)

```bash
PYTHONPATH=. SPIDER_ACCOUNT=<<spider account>> SPIDER_USER=<<YOUR SPIDER USERNAME>> SPIDER_PWD='<<YOUR_PASSWORD>>' python merge_databases.py --output_database=SPIDER2_MERGED_250922  --dry_run=True
```

## 4. Share data

Create a share using [Data sharing](https://docs.snowflake.com/en/user-guide/data-sharing-provider#using-sql-to-create-a-share) on source account.
Accept share on destination account [docs](https://docs.snowflake.com/en/user-guide/data-share-consumers#viewing-available-shares)

## 5. Unpack databases

When share is complete, you can unpack databases to their original structure. This requires a few sub-steps.

### 5.1 Database mapping

Generate mapping from merged database schemas to individual target databases.

```bash
export SNOWFLAKE_ACCOUNT=xxx-yyy
export SNOWFLAKE_ROLE=
export SNOWFLAKE_WAREHOUSE=
export SNOWFLAKE_ADMIN_ROLE=
export SNOWFLAKE_READONLY_ROLE=
export DESTINATION_USER=
export SNOWFLAKE_PASSWORD=

python database_mapping.py SPIDER2_MERGED_250922 spider2_mapping.jsonl
```

### 5.2 Database creation

Create target databases with proper permissions. This step requires administrative privileges.


```bash
export SNOWFLAKE_ACCOUNT=xxx-yyy
export SNOWFLAKE_ROLE=
export SNOWFLAKE_WAREHOUSE=
export SNOWFLAKE_ADMIN_ROLE=
export SNOWFLAKE_READONLY_ROLE=
export DESTINATION_USER=
export SNOWFLAKE_PASSWORD=

python create_databases.py spider2_mapping.jsonl \
  --account=YOUR_ACCOUNT \
  --admin_role=YOUR_ADMIN_ROLE \
  --readonly_role=YOUR_READONLY_ROLE
```

### 5.3 Table creation

Create tables by copying data from the shared database (cloning is not supported for shared data).

```bash
export SNOWFLAKE_ACCOUNT=xxx-yyy
export SNOWFLAKE_ROLE=
export SNOWFLAKE_WAREHOUSE=
export SNOWFLAKE_ADMIN_ROLE=
export SNOWFLAKE_READONLY_ROLE=
export DESTINATION_USER=
export SNOWFLAKE_PASSWORD=

python create_tables.py spider2_mapping.jsonl \
  --account=YOUR_ACCOUNT \
  --role=YOUR_ROLE \
  --warehouse=YOUR_WAREHOUSE
```
