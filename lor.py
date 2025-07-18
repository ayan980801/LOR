from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Any
import logging
from functools import wraps
import re
import sys
import traceback
from datetime import datetime
import pytz
import asyncio
import nest_asyncio
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    sha2,
    concat_ws,
    col,
    when,
    trim,
    regexp_replace,
    upper,
    to_timestamp,
    year,
    coalesce,
)
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    TimestampType,
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    DateType,
)
from delta.tables import DeltaTable
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    RetryError,
    retry_if_exception_type,
)

# Apply nest_asyncio for Azure Databricks compatibility
nest_asyncio.apply()

# ========== CONFIGURATION: CHANGE ETL MODE HERE ==============
# Set to "historical" for full reload or "incremental" for delta loads
ETL_MODE = "historical"  
# =============================================================


def log_exceptions(default=None, exit_on_error=False):
    """Decorator for standardized exception logging."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                logging.error(f"Error in {func.__name__}: {exc}")
                logging.error(traceback.format_exc())
                if exit_on_error:
                    sys.exit(1)
                return default
        return wrapper
    return decorator


def async_log_exceptions(default=None, exit_on_error=False):
    """Async decorator for standardized exception logging."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as exc:
                logging.error(f"Error in {func.__name__}: {exc}")
                logging.error(traceback.format_exc())
                if exit_on_error:
                    sys.exit(1)
                return default
        return wrapper
    return decorator


@dataclass
class ServerDetails:
    server_url: str
    db_name: str
    username: str
    password: str
    fetchsize: str = "50000"
    bulkCopyBatchSize: str = "10000"


@dataclass
class AzureDetails:
    base_path: str
    stage: str
    db: str


@dataclass
class SnowflakeConfig:
    sfURL: str
    sfUser: str
    sfPassword: str
    sfDatabase: str
    sfWarehouse: str
    sfSchema: str
    sfRole: str


class ConfigManager:
    """Centralized configuration management for LOR ETL following LeadDepot patterns."""
    
    TIMEZONE = "America/New_York"
    
    # Azure Data Lake paths - matching LeadDepot pattern
    ADLS_RAW_BASE_PATH = "abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/RAW/LOR"
    
    # LOR Tables configuration with Snowflake mappings - FIXED to follow LeadDepot staging pattern
    LOR_TABLES: Dict[str, Dict[str, str]] = {
        "Lead": {
            "snowflake_table": "STG_LOR_LEAD",
            "hash_column_name": "STG_LOR_LEAD_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LOR_LEAD",
        },
        "LeadAgent": {
            "snowflake_table": "STG_LOR_LEAD_AGENT",
            "hash_column_name": "STG_LOR_LEAD_AGENT_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LOR_LEAD_AGENT",
        },
        "LeadAllocation": {
            "snowflake_table": "STG_LOR_LEAD_ALLOCATION",
            "hash_column_name": "STG_LOR_LEAD_ALLOCATION_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LOR_LEAD_ALLOCATION",
        },
        "LeadOrder": {
            "snowflake_table": "STG_LOR_LEAD_ORDER",
            "hash_column_name": "STG_LOR_LEAD_ORDER_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LOR_LEAD_ORDER",
        },
        "LeadOrderHistory": {
            "snowflake_table": "STG_LOR_LEAD_ORDER_HISTORY",
            "hash_column_name": "STG_LOR_LEAD_ORDER_HISTORY_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LOR_LEAD_ORDER_HISTORY",
        },
        "LeadOrderLine": {
            "snowflake_table": "STG_LOR_LEAD_ORDER_LINE",
            "hash_column_name": "STG_LOR_LEAD_ORDER_LINE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LOR_LEAD_ORDER_LINE",
        },
    }
    
    # Metadata columns that should be excluded from hashing
    METADATA_COLUMNS = {
        "ETL_CREATED_DATE",
        "ETL_LAST_UPDATE_DATE",
        "CREATED_BY",
        "TO_PROCESS",
        "EDW_EXTERNAL_SOURCE_SYSTEM",
        "ETL_BATCH_ID",
        "ETL_ROW_HASH",
    }
    
    # SQL Server connection settings
    SQL_SERVER_SETTINGS = {
        "fetchsize": "50000",
        "bulkCopyBatchSize": "10000",
        "queryTimeout": "0",
        "loginTimeout": "60",
        "lockTimeout": "10000",
        "encrypt": "true",
        "trustServerCertificate": "true",
    }

    @staticmethod
    def get_spark():
        """Get or create SparkSession with optimized configurations."""
        return (
            SparkSession.builder.appName("LORUnifiedETL")
            .config(
                "spark.jars.packages",
                ",".join([
                    "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8",
                    "net.snowflake:snowflake-jdbc:3.13.8",
                    "net.snowflake:spark-snowflake_2.12:2.9.3",
                ])
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config(
                "spark.databricks.delta.properties.defaults.columnMapping.mode", "name"
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate()
        )


class DataTransformations:
    """Centralized data transformation utilities following LeadDepot patterns."""
    
    @staticmethod
    def transform_column_name(col_name: str) -> str:
        """Convert camelCase to UPPER_SNAKE_CASE."""
        # Handle sequences of capitals followed by lowercase
        words = re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?![a-z])|[0-9]+', col_name)
        return '_'.join(words).upper()
    
    @staticmethod
    def clean_column_names(df: DataFrame) -> DataFrame:
        """Clean and standardize column names to UPPER_SNAKE_CASE."""
        original_cols = df.columns
        cleaned_cols = []
        
        for c in original_cols:
            # Apply camelCase to snake_case transformation
            cleaned = DataTransformations.transform_column_name(c)
            # Additional cleaning
            cleaned = re.sub(r'[^\w]', '_', cleaned)
            cleaned = re.sub(r'_+', '_', cleaned)
            cleaned = cleaned.strip('_')
            cleaned = cleaned.upper()
            cleaned_cols.append(cleaned)
        
        # Handle duplicates
        if len(set(cleaned_cols)) != len(cleaned_cols):
            seen = {}
            final_cols = []
            for col in cleaned_cols:
                if col in seen:
                    seen[col] += 1
                    final_cols.append(f"{col}_{seen[col]}")
                else:
                    seen[col] = 0
                    final_cols.append(col)
            cleaned_cols = final_cols
        
        return df.toDF(*cleaned_cols)
    
    @staticmethod
    def validate_timestamps(df: DataFrame) -> DataFrame:
        """Validate and clean timestamp columns."""
        timestamp_cols = [
            field.name for field in df.schema.fields
            if isinstance(field.dataType, TimestampType)
        ]
        
        current_year = datetime.now().year
        current_ts = current_timestamp()
        
        for ts_col in timestamp_cols:
            # Clean invalid timestamps
            df = df.withColumn(
                ts_col,
                when(
                    col(ts_col).isNull() 
                    | (year(col(ts_col)) < 1900)
                    | (year(col(ts_col)) > current_year + 1)
                    | (col(ts_col) > current_ts),
                    lit(None)
                ).otherwise(col(ts_col))
            )
            
            # For ETL columns, use current timestamp as default
            if ts_col.startswith("ETL_"):
                df = df.withColumn(
                    ts_col,
                    when(col(ts_col).isNull(), current_ts).otherwise(col(ts_col))
                )
        
        return df
    
    @staticmethod
    def add_row_hash(df: DataFrame, table_name: str) -> DataFrame:
        """
        Add deterministic hash column using SHA-512 (matching LeadDepot).
        This hash is used for change detection in delta append scenarios.
        """
        hash_column = ConfigManager.LOR_TABLES.get(table_name, {}).get("hash_column_name")
        if not hash_column:
            logging.info(f"No hash column configured for table {table_name}. Skipping hash generation.")
            return df
        
        # Get all columns except metadata columns and the hash column itself
        columns_to_hash = [
            c for c in df.columns 
            if c not in ConfigManager.METADATA_COLUMNS and c != hash_column
        ]
        
        if not columns_to_hash:
            logging.warning(f"No columns available for hashing in table {table_name}.")
            df = df.withColumn(hash_column, sha2(lit(""), 512))
        else:
            # Sort columns for consistent hashing
            columns_to_hash.sort()
            # Create deterministic hash using SHA-512 (not SHA3-512)
            # Use coalesce to handle nulls consistently
            hash_cols = [coalesce(col(c).cast("string"), lit("")) for c in columns_to_hash]
            df = df.withColumn(
                hash_column,
                sha2(concat_ws("||", *hash_cols), 512)
            )
        
        # Add ETL_ROW_HASH for internal tracking
        df = df.withColumn("ETL_ROW_HASH", col(hash_column))
        
        # Reorder columns with hash first
        new_col_order = [hash_column] + [c for c in df.columns if c != hash_column]
        df = df.select(new_col_order)
        
        return df
    
    @staticmethod
    def add_metadata_columns(df: DataFrame, batch_id: str = None) -> DataFrame:
        """Add standard metadata columns for ETL tracking."""
        if batch_id is None:
            batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        return (
            df.withColumn("ETL_CREATED_DATE", current_timestamp())
            .withColumn("ETL_LAST_UPDATE_DATE", current_timestamp())
            .withColumn("CREATED_BY", lit("LOR_ETL_PROCESS"))
            .withColumn("TO_PROCESS", lit(True))
            .withColumn("EDW_EXTERNAL_SOURCE_SYSTEM", lit("LOR"))
            .withColumn("ETL_BATCH_ID", lit(batch_id))
        )


class AsyncLORETL:
    """Async LOR ETL pipeline following LeadDepot patterns."""
    
    log_exceptions = staticmethod(log_exceptions)
    async_log_exceptions = staticmethod(async_log_exceptions)
    
    def __init__(self, dbutils=None) -> None:
        self.dbutils = dbutils or self._get_dbutils()
        self._configure_logging()
        self.spark = ConfigManager.get_spark()
        self.config = ConfigManager()
        self.transformations = DataTransformations()
        self.server_details = self._load_server_details()
        self.snowflake_config = self._load_snowflake_config()
        
        # Use the global ETL_MODE constant
        self.etl_mode = ETL_MODE.lower()
        
        # Validate ETL mode
        valid_modes = ("historical", "incremental")
        if self.etl_mode not in valid_modes:
            raise ValueError(
                f"ETL_MODE must be either 'historical' or 'incremental'. "
                f"Got: {repr(ETL_MODE)} in the configuration at the top of the file."
            )
        
        logging.info(f"ETL run mode is set to: {self.etl_mode.upper()}")
        
        # Generate batch ID for this run
        self.batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        logging.info(f"ETL batch ID: {self.batch_id}")
        
        # Thread pool for blocking operations
        self._executor = ThreadPoolExecutor(max_workers=6)
        
        # Table existence cache
        self._table_exists_cache = {}
        
    @staticmethod
    def _configure_logging() -> None:
        """Configure structured logging."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )
        # Suppress verbose Spark logs
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)
    
    @staticmethod
    def _log_retry(retry_state) -> None:
        """Log retry attempts."""
        exc = retry_state.outcome.exception()
        attempt = retry_state.attempt_number
        func_name = (
            retry_state.fn.__name__ if hasattr(retry_state, "fn") else "operation"
        )
        logging.warning(
            f"Retry {attempt} for {func_name} due to {type(exc).__name__}: {str(exc)}"
            if exc
            else f"Retry {attempt} for {func_name}"
        )
    
    @staticmethod
    def _get_dbutils():
        """Get dbutils instance if available."""
        try:
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            logging.warning("dbutils not available in this environment")
            return None
    
    @log_exceptions(exit_on_error=True)
    def _load_server_details(self) -> ServerDetails:
        """Load SQL Server connection details for LOR."""
        if not self.dbutils:
            raise ValueError("dbutils is required to fetch SQL Server secrets")
        return ServerDetails(
            server_url="sqlsrv-lor-prod.database.windows.net:1433",
            db_name="sqlsrv-qlm-prod",
            username=self.dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-LOR-User-PROD"
            ),
            password=self.dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-LOR-Pass-PROD"
            ),
            fetchsize=ConfigManager.SQL_SERVER_SETTINGS["fetchsize"],
            bulkCopyBatchSize=ConfigManager.SQL_SERVER_SETTINGS["bulkCopyBatchSize"],
        )
    
    @log_exceptions(exit_on_error=True)
    def _load_snowflake_config(self) -> SnowflakeConfig:
        """Load Snowflake connection configuration."""
        if not self.dbutils:
            raise ValueError("dbutils is required to fetch Snowflake secrets")
        return SnowflakeConfig(
            sfURL="https://hmkovlx-nu26765.snowflakecomputing.com",
            sfUser=self.dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SF-EDW-User"
            ),
            sfPassword=self.dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SF-EDW-Pass"
            ),
            sfDatabase="DEV",
            sfWarehouse="INTEGRATION_COMPUTE_WH",
            sfSchema="QUILITY_EDW_STAGE",
            sfRole="ACCOUNTADMIN",
        )
    
    @staticmethod
    def _retry() -> Dict[str, object]:
        """Get retry configuration."""
        return dict(
            stop=stop_after_attempt(3),
            wait=wait_exponential(min=1, max=8, multiplier=1),
            before_sleep=AsyncLORETL._log_retry,
            reraise=True,
        )
    
    async def _read_sql_server_with_retry(self, query: str) -> DataFrame:
        """Async read from SQL Server with retry logic."""
        @retry(**self._retry())
        async def _read() -> DataFrame:
            jdbc_url = (
                f"jdbc:sqlserver://{self.server_details.server_url};"
                f"databaseName={self.server_details.db_name};"
                f"encrypt={ConfigManager.SQL_SERVER_SETTINGS['encrypt']};"
                f"trustServerCertificate={ConfigManager.SQL_SERVER_SETTINGS['trustServerCertificate']};"
                f"queryTimeout={ConfigManager.SQL_SERVER_SETTINGS['queryTimeout']};"
                f"loginTimeout={ConfigManager.SQL_SERVER_SETTINGS['loginTimeout']};"
                f"lockTimeout={ConfigManager.SQL_SERVER_SETTINGS['lockTimeout']}"
            )
            
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self._executor,
                lambda: (
                    self.spark.read.format("jdbc")
                    .option("url", jdbc_url)
                    .option("dbtable", f"({query}) as src")
                    .option("user", self.server_details.username)
                    .option("password", self.server_details.password)
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    .option("fetchsize", self.server_details.fetchsize)
                    .option("bulkCopyBatchSize", self.server_details.bulkCopyBatchSize)
                    .option("batchsize", "10000")
                    .option("isolationLevel", "READ_UNCOMMITTED")
                    .option("tableLock", "false")
                    .load()
                )
            )
        
        try:
            return await _read()
        except RetryError as exc:
            logging.error(f"SQL Server read failed after retries: {query}")
            raise exc.last_attempt.exception()
    
    async def _check_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in Snowflake."""
        if table_name in self._table_exists_cache:
            return self._table_exists_cache[table_name]
        
        # Extract parts from table name
        parts = table_name.split('.')
        if len(parts) == 3:
            database, schema, table = parts
        else:
            database = self.snowflake_config.sfDatabase
            schema = self.snowflake_config.sfSchema
            table = table_name
        
        check_query = f"""
            SELECT COUNT(*) AS CNT 
            FROM {database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema}' 
            AND TABLE_NAME = '{table}'
        """
        
        try:
            loop = asyncio.get_event_loop()
            result_df = await loop.run_in_executor(
                self._executor,
                lambda: (
                    self.spark.read.format("snowflake")
                    .options(**self.snowflake_config.__dict__)
                    .option("query", check_query)
                    .load()
                )
            )
            
            count = await loop.run_in_executor(
                None,
                lambda: result_df.collect()[0].CNT
            )
            
            exists = count > 0
            self._table_exists_cache[table_name] = exists
            return exists
            
        except Exception as e:
            logging.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    async def _write_snowflake_with_retry(
        self, df: DataFrame, table: str, mode: str = "append", truncate: bool = False
    ) -> None:
        """Async write to Snowflake with retry logic."""
        @retry(**self._retry())
        async def _write() -> None:
            loop = asyncio.get_event_loop()
            
            # Build write operation
            writer = (
                df.write.format("snowflake")
                .options(**self.snowflake_config.__dict__)
                .option("dbtable", table)
                .option("on_error", "CONTINUE")
                .option("column_mapping", "name")
                .option("keep_column_case", "true")
                .option("usestagingtable", "off")
            )
            
            # Add truncate option if needed
            if truncate and mode == "append":
                writer = writer.option("truncate_table", "on")
            else:
                writer = writer.option("truncate_table", "off")
            
            # Set mode and save
            await loop.run_in_executor(
                self._executor,
                lambda: writer.mode(mode).save()
            )
        
        try:
            await _write()
        except RetryError as exc:
            logging.error(f"Snowflake write failed after retries for table: {table}")
            raise exc.last_attempt.exception()
    
    async def _read_snowflake_with_retry(self, query: str) -> DataFrame:
        """Async read from Snowflake with retry logic."""
        @retry(**self._retry())
        async def _read() -> DataFrame:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self._executor,
                lambda: (
                    self.spark.read.format("snowflake")
                    .options(**self.snowflake_config.__dict__)
                    .option("query", query)
                    .load()
                )
            )
        
        try:
            return await _read()
        except RetryError as exc:
            logging.error(f"Snowflake read failed after retries: {query}")
            raise exc.last_attempt.exception()
    
    @async_log_exceptions(default=None)
    async def extract_table(self, table_name: str) -> Optional[DataFrame]:
        """Asynchronously extract data from a SQL Server table."""
        logging.info(f"Extracting data from table: {table_name}")
        
        # Build query based on ETL mode
        if self.etl_mode == "incremental":
            # For incremental mode, we still select all data but will handle
            # deduplication downstream since we don't have reliable timestamps
            query = f"SELECT * FROM dbo.[{table_name}] WITH (NOLOCK)"
            logging.info(f"Incremental mode: Extracting all records (deduplication deferred to downstream)")
        else:
            # Historical mode - full extract
            query = f"SELECT * FROM dbo.[{table_name}] WITH (NOLOCK)"
            logging.info(f"Historical mode: Extracting all records from {table_name}")
        
        # Get record count first
        count_query = f"SELECT COUNT(*) AS total_count FROM dbo.[{table_name}] WITH (NOLOCK)"
        count_df = await self._read_sql_server_with_retry(count_query)
        
        loop = asyncio.get_event_loop()
        total_count = await loop.run_in_executor(
            None,
            lambda: count_df.collect()[0].total_count
        )
        logging.info(f"Total records in source table {table_name}: {total_count:,}")
        
        if total_count == 0:
            logging.warning(f"Table {table_name} is empty. Skipping.")
            return None
        
        # Extract data
        df = await self._read_sql_server_with_retry(query)
        
        # Clean column names
        df = self.transformations.clean_column_names(df)
        
        extracted_count = await loop.run_in_executor(None, df.count)
        logging.info(f"Successfully extracted {extracted_count:,} records from {table_name}")
        
        return df
    
    @async_log_exceptions()
    async def write_to_adls_raw(self, df: DataFrame, table_name: str) -> None:
        """Write to RAW layer in ADLS (original data)."""
        path = f"{ConfigManager.ADLS_RAW_BASE_PATH}/{table_name}"
        logging.info(f"Writing raw data to ADLS: {table_name} -> {path}")
        
        loop = asyncio.get_event_loop()
        is_empty = await loop.run_in_executor(None, df.rdd.isEmpty)
        
        if is_empty:
            logging.warning(f"DataFrame for table {table_name} is empty. Skipping ADLS write.")
            return
        
        # For incremental mode, we append to existing data
        if self.etl_mode == "incremental":
            mode = "append"
            logging.info(f"Incremental mode: Appending data to {path}")
        else:
            mode = "overwrite"
            logging.info(f"Historical mode: Overwriting data at {path}")
        
        await loop.run_in_executor(
            self._executor,
            lambda: df.write.format("delta")
            .option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
            .option("mergeSchema", "true")
            .option("overwriteSchema", "true" if mode == "overwrite" else "false")
            .mode(mode)
            .save(path)
        )
        
        # Verify write
        written_df = await loop.run_in_executor(
            None,
            lambda: self.spark.read.format("delta").load(path)
        )
        written_count = await loop.run_in_executor(None, written_df.count)
        logging.info(f"Verified {written_count:,} total records in RAW layer for {table_name}")
    
    async def validate_dataframe(self, df: DataFrame, table_name: str) -> bool:
        """Asynchronously validate DataFrame before writing to Snowflake."""
        loop = asyncio.get_event_loop()
        
        is_empty = await loop.run_in_executor(None, df.rdd.isEmpty)
        if is_empty:
            logging.warning(f"DataFrame for {table_name} is empty")
            return False
        
        # Check for required metadata columns
        missing_metadata = ConfigManager.METADATA_COLUMNS - set(df.columns)
        if missing_metadata:
            logging.error(f"Missing metadata columns in {table_name}: {missing_metadata}")
            return False
        
        # Check for hash column
        hash_column = ConfigManager.LOR_TABLES.get(table_name, {}).get("hash_column_name")
        if hash_column and hash_column not in df.columns:
            logging.error(f"Missing hash column {hash_column} in {table_name}")
            return False
        
        return True
    
    @async_log_exceptions()
    async def load_to_snowflake(self, df: DataFrame, table_name: str, table_config: Dict[str, str]) -> None:
        """Asynchronously load DataFrame to Snowflake."""
        snowflake_table = table_config["staging_table_name"]
        snowflake_table_name_only = snowflake_table.split('.')[-1]
        
        is_valid = await self.validate_dataframe(df, table_name)
        if not is_valid:
            logging.error(f"DataFrame validation failed for {table_name}. Skipping Snowflake load.")
            return
        
        loop = asyncio.get_event_loop()
        row_count = await loop.run_in_executor(None, df.count)
        logging.info(f"Loading {row_count:,} records to Snowflake table: {snowflake_table}")
        
        # Check if table exists
        table_exists = await self._check_table_exists(snowflake_table)
        
        if self.etl_mode == "historical":
            if table_exists:
                # For historical mode with existing table, truncate and append
                logging.info(f"Historical load: Table {snowflake_table} exists. Truncating and loading...")
                await self._write_snowflake_with_retry(
                    df, 
                    snowflake_table_name_only, 
                    mode="append", 
                    truncate=True
                )
            else:
                # For historical mode without existing table, use overwrite to create
                logging.info(f"Historical load: Creating new table {snowflake_table}...")
                await self._write_snowflake_with_retry(
                    df, 
                    snowflake_table_name_only, 
                    mode="overwrite", 
                    truncate=False
                )
        else:
            # Incremental mode
            if table_exists:
                # Table exists - just append
                logging.info(f"Incremental load: Appending data to {snowflake_table}")
                await self._write_snowflake_with_retry(
                    df, 
                    snowflake_table_name_only, 
                    mode="append", 
                    truncate=False
                )
            else:
                # Table doesn't exist - create with overwrite
                logging.info(f"Incremental load: Creating new table {snowflake_table}...")
                await self._write_snowflake_with_retry(
                    df, 
                    snowflake_table_name_only, 
                    mode="overwrite", 
                    truncate=False
                )
        
        # Update cache
        self._table_exists_cache[snowflake_table] = True
        
        # Validate load
        validation_query = f"SELECT COUNT(*) AS CNT FROM {snowflake_table}"
        try:
            snowflake_df = await self._read_snowflake_with_retry(validation_query)
            snowflake_count = await loop.run_in_executor(
                None,
                lambda: snowflake_df.collect()[0].CNT
            )
            
            logging.info(
                f"Snowflake table {snowflake_table} now contains {snowflake_count:,} records "
                f"(loaded {row_count:,} in this run)"
            )
        except Exception as e:
            logging.warning(f"Could not validate record count in Snowflake: {e}")
    
    @async_log_exceptions()
    async def process_table(self, table_name: str) -> Dict[str, Any]:
        """Asynchronously process a single table through the ETL pipeline."""
        start_time = datetime.now()
        logging.info(f"{'='*60}")
        logging.info(f"Processing LOR table: {table_name}")
        logging.info(f"Mode: {self.etl_mode.upper()}")
        logging.info(f"Batch ID: {self.batch_id}")
        logging.info(f"{'='*60}")
        
        try:
            # Extract from SQL Server
            df = await self.extract_table(table_name)
            if df is None:
                return {"table": table_name, "status": "skipped", "reason": "empty"}
            
            # Transform data
            loop = asyncio.get_event_loop()
            
            # Add hash column (SHA-512 for change detection)
            df = await loop.run_in_executor(
                None,
                lambda: self.transformations.add_row_hash(df, table_name)
            )
            
            # Add metadata columns
            df = await loop.run_in_executor(
                None,
                lambda: self.transformations.add_metadata_columns(df, self.batch_id)
            )
            
            # Validate timestamps
            df = await loop.run_in_executor(
                None,
                lambda: self.transformations.validate_timestamps(df)
            )
            
            # Write to ADLS and Snowflake concurrently
            tasks = []
            
            # Write raw data to ADLS
            tasks.append(self.write_to_adls_raw(df, table_name))
            
            # Load to Snowflake if configured
            if table_name in ConfigManager.LOR_TABLES:
                tasks.append(self.load_to_snowflake(df, table_name, ConfigManager.LOR_TABLES[table_name]))
            else:
                logging.warning(f"Table {table_name} is not configured for Snowflake loading")
            
            # Execute tasks concurrently
            await asyncio.gather(*tasks)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logging.info(f"Successfully processed {table_name} in {elapsed:.2f} seconds")
            
            return {
                "table": table_name,
                "status": "success",
                "elapsed": elapsed,
                "batch_id": self.batch_id
            }
            
        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            logging.error(f"Failed to process {table_name} after {elapsed:.2f} seconds: {str(e)}")
            logging.error(traceback.format_exc())
            return {
                "table": table_name,
                "status": "failed",
                "reason": str(e),
                "elapsed": elapsed,
                "batch_id": self.batch_id
            }
    
    async def run(self) -> None:
        """Run the complete async ETL pipeline."""
        pipeline_start = datetime.now()
        logging.info("="*80)
        logging.info(f"Starting LOR Async ETL Pipeline")
        logging.info(f"Mode: {self.etl_mode.upper()}")
        logging.info(f"Batch ID: {self.batch_id}")
        logging.info("="*80)
        
        # Get tables to process
        tables_to_process = list(ConfigManager.LOR_TABLES.keys())
        logging.info(f"Tables to process: {', '.join(tables_to_process)}")
        
        # Process tables concurrently with semaphore to limit concurrency
        semaphore = asyncio.Semaphore(3)  # Limit to 3 concurrent table processing
        
        async def process_with_semaphore(table: str) -> Dict[str, Any]:
            async with semaphore:
                return await self.process_table(table)
        
        # Create tasks for all tables
        tasks = [process_with_semaphore(table) for table in tables_to_process]
        
        # Process all tables concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Analyze results
        success_count = 0
        failure_count = 0
        skipped_count = 0
        failed_tables = []
        
        for table, result in zip(tables_to_process, results):
            if isinstance(result, Exception):
                failure_count += 1
                failed_tables.append(table)
                logging.error(f"Table {table} raised exception: {result}")
            elif isinstance(result, dict):
                if result["status"] == "success":
                    success_count += 1
                elif result["status"] == "skipped":
                    skipped_count += 1
                else:
                    failure_count += 1
                    failed_tables.append(table)
            else:
                failure_count += 1
                failed_tables.append(table)
                logging.error(f"Table {table} returned unexpected result: {result}")
        
        # Summary
        elapsed = (datetime.now() - pipeline_start).total_seconds()
        logging.info("="*80)
        logging.info("LOR ETL Pipeline Summary:")
        logging.info(f"  Mode: {self.etl_mode.upper()}")
        logging.info(f"  Batch ID: {self.batch_id}")
        logging.info(f"  Total tables: {len(tables_to_process)}")
        logging.info(f"  Successful: {success_count}")
        logging.info(f"  Failed: {failure_count}")
        logging.info(f"  Skipped: {skipped_count}")
        logging.info(f"  Total time: {elapsed:.2f} seconds")
        logging.info("="*80)
        
        if failure_count > 0:
            logging.error(f"ETL completed with {failure_count} failures: {failed_tables}")
            sys.exit(1)
        else:
            logging.info("ETL completed successfully")
    
    def __del__(self):
        """Cleanup resources."""
        if hasattr(self, '_executor'):
            self._executor.shutdown(wait=False)


async def async_main() -> None:
    """Async main entry point."""
    etl = AsyncLORETL()
    await etl.run()


@log_exceptions(exit_on_error=True)
def main() -> None:
    """Main entry point with async wrapper."""
    # Create and run the event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(async_main())
    finally:
        loop.close()


if __name__ == "__main__":
    # Run the ETL pipeline
    main()
