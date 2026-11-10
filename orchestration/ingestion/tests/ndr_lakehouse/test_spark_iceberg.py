"""Tests for the Delta UniForm conversion layer (no Spark/Delta runtime required)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ndr_lakehouse.domain.errors import UnsupportedWriteModeError
from ndr_lakehouse.iceberg import (
    AppendWriteStrategy,
    DataFrameTransformer,
    DeltaMergeExecutor,
    MergeWriteStrategy,
    NativeDatasetReader,
    OverwriteWriteStrategy,
    SchemaEvolutionService,
    SparkSessionFactory,
    SparkSettings,
    WriteStrategyFactory,
    sanitize_column_name,
    sanitize_columns,
)
from ndr_lakehouse.iceberg.properties import (
    TablePropertiesSynchronizer,
    with_uniform_iceberg_properties,
)
from ndr_lakehouse.iceberg.service import IcebergLoadService, SnapshotInspector


def _settings() -> SparkSettings:
    return SparkSettings(
        endpoint_url="http://rustfs:9000",
        access_key_id="rustfsadmin",
        secret_access_key="rustfssecret",
        uc_iceberg_uri="http://unity-catalog:8080/api/2.1/unity-catalog/iceberg",
        warehouse="s3://lakehouse/warehouse",
    )


def test_sanitize_column_name_matches_contract():
    assert sanitize_column_name("Code Commune (INSEE)") == "code_commune__insee_"
    assert sanitize_column_name("Valeur foncière") == "valeur_fonci_re"
    assert sanitize_column_name("2024") == "_2024"
    assert sanitize_column_name("") == "_unnamed"


def test_sanitize_columns_disambiguates_collisions(mocker):
    df = mocker.MagicMock(name="dataframe")
    df.columns = ["Code Commune", "Code-Commune", "id"]
    df.toDF.return_value = "renamed"

    result = sanitize_columns(df)

    assert result == "renamed"
    df.toDF.assert_called_once_with("code_commune", "code_commune_1", "id")


def test_evolve_target_schema_emits_alter_table_add_column(mocker):
    spark = mocker.MagicMock(name="spark")
    spark.table.return_value.columns = ["id"]
    df = mocker.MagicMock(name="dataframe")
    df.schema.fields = [
        SimpleNamespace(
            name="id",
            dataType=SimpleNamespace(simpleString=lambda: "bigint"),
        ),
        SimpleNamespace(
            name="new_column",
            dataType=SimpleNamespace(simpleString=lambda: "string"),
        ),
    ]

    added = SchemaEvolutionService().evolve(spark, df, "lakehouse.bronze.demo_table")

    assert added == ["new_column"]
    spark.sql.assert_called_once_with(
        "ALTER TABLE lakehouse.bronze.demo_table ADD COLUMN new_column string"
    )


def _merge_catalog(sample_asset_config, **updates):
    catalog = sample_asset_config.catalog_properties.model_copy(
        update={"write_mode": "merge", "primary_keys": ["k1", "k2"], **updates}
    )
    return catalog


def _merge_mocks(mocker, df):
    source_alias = mocker.MagicMock(name="source-alias")
    df.alias.return_value = source_alias
    target_table = mocker.MagicMock(name="delta-table")
    target_alias = mocker.MagicMock(name="target-alias")
    target_table.alias.return_value = target_alias
    merge = mocker.MagicMock(name="merge-builder")
    target_alias.merge.return_value = merge
    merge.whenMatchedUpdate.return_value = merge
    merge.whenMatchedDelete.return_value = merge
    merge.whenNotMatchedInsert.return_value = merge
    merge.whenNotMatchedBySourceDelete.return_value = merge
    resolver = mocker.MagicMock(name="resolver", return_value=target_table)
    return target_table, target_alias, source_alias, merge, resolver


def test_delta_merge_uses_composite_key(mocker, sample_asset_config):
    catalog = _merge_catalog(sample_asset_config)
    df = mocker.MagicMock(name="dataframe")
    df.columns = ["k1", "k2", "value"]
    target_table, target_alias, source_alias, merge, resolver = _merge_mocks(mocker, df)

    condition = DeltaMergeExecutor(table_resolver=resolver).execute(mocker.MagicMock(), df, catalog)

    assert condition == "target.k1 = source.k1 AND target.k2 = source.k2"
    target_table.alias.assert_called_once_with("target")
    target_alias.merge.assert_called_once_with(
        source=source_alias,
        condition="target.k1 = source.k1 AND target.k2 = source.k2",
    )
    merge.whenMatchedUpdate.assert_called_once_with(set={"value": "source.value"})
    merge.whenNotMatchedInsert.assert_called_once_with(
        values={
            "k1": "source.k1",
            "k2": "source.k2",
            "value": "source.value",
        }
    )
    merge.execute.assert_called_once()


def test_delta_merge_supports_delete_conditions(mocker, sample_asset_config):
    catalog = _merge_catalog(
        sample_asset_config,
        merge_delete_condition="source.value IS NULL",
        merge_matched_update_condition="source.value > target.value",
        merge_not_matched_insert_condition="source.value IS NOT NULL",
        merge_not_matched_by_source_delete_condition="target.value < 0",
    )
    df = mocker.MagicMock(name="dataframe")
    df.columns = ["k1", "k2", "value"]
    _, _, _, merge, resolver = _merge_mocks(mocker, df)

    DeltaMergeExecutor(table_resolver=resolver).execute(mocker.MagicMock(), df, catalog)

    merge.whenMatchedDelete.assert_called_once_with(condition="source.value IS NULL")
    merge.whenMatchedUpdate.assert_called_once_with(
        set={"value": "source.value"},
        condition="source.value > target.value",
    )
    merge.whenNotMatchedInsert.assert_called_once_with(
        values={
            "k1": "source.k1",
            "k2": "source.k2",
            "value": "source.value",
        },
        condition="source.value IS NOT NULL",
    )
    merge.whenNotMatchedBySourceDelete.assert_called_once_with(condition="target.value < 0")


def test_build_merge_condition_returns_composite_clause():
    assert (
        DeltaMergeExecutor.build_merge_condition(["a", "b"])
        == "target.a = source.a AND target.b = source.b"
    )


def test_write_overwrite_creates_uniform_delta_table(mocker, sample_asset_config):
    catalog = sample_asset_config.catalog_properties
    creator = mocker.MagicMock(name="delta-creator")
    builder = mocker.MagicMock(name="delta-builder")
    creator.return_value = builder
    builder.tableName.return_value = builder
    builder.addColumns.return_value = builder
    builder.partitionedBy.return_value = builder
    builder.property.return_value = builder

    df = mocker.MagicMock(name="dataframe")
    writer = mocker.MagicMock(name="dataframe-writer")
    df.write.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer

    OverwriteWriteStrategy(table_creator=creator).write(mocker.MagicMock(), df, catalog)

    creator.assert_called_once()
    builder.tableName.assert_called_once_with(catalog.full_table_name)
    builder.addColumns.assert_called_once_with(df.schema)
    builder.partitionedBy.assert_called_once_with(*catalog.partition_by)
    properties = {call.args[0]: call.args[1] for call in builder.property.call_args_list}
    assert properties["delta.enableIcebergCompatV2"] == "true"
    assert properties["delta.universalFormat.enabledFormats"] == "iceberg"
    df.write.format.assert_called_once_with("delta")
    writer.mode.assert_called_once_with("append")
    writer.option.assert_called_once_with("mergeSchema", "true")
    writer.saveAsTable.assert_called_once_with(catalog.full_table_name)
    builder.execute.assert_called_once()


def test_append_write_strategy_uses_merge_schema(mocker, sample_asset_config):
    df = mocker.MagicMock(name="dataframe")
    writer = mocker.MagicMock(name="dataframe-writer")
    df.write.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer

    AppendWriteStrategy().write(mocker.MagicMock(), df, sample_asset_config.catalog_properties)

    df.write.format.assert_called_once_with("delta")
    writer.mode.assert_called_once_with("append")
    writer.option.assert_called_once_with("mergeSchema", "true")
    writer.saveAsTable.assert_called_once_with(
        sample_asset_config.catalog_properties.full_table_name
    )


def test_merge_write_strategy_delegates_to_delta_executor(mocker, sample_asset_config):
    executor = mocker.MagicMock(name="delta-merge-executor")
    strategy = MergeWriteStrategy(executor)

    strategy.write(
        mocker.MagicMock(),
        mocker.MagicMock(),
        sample_asset_config.catalog_properties,
    )

    executor.execute.assert_called_once()


def test_write_strategy_factory_rejects_unknown_mode():
    with pytest.raises(UnsupportedWriteModeError):
        WriteStrategyFactory.default().create("upsert")


def test_uniform_properties_are_always_enabled():
    merged = with_uniform_iceberg_properties({"custom": "value"})
    assert merged == {
        "custom": "value",
        "delta.enableIcebergCompatV2": "true",
        "delta.universalFormat.enabledFormats": "iceberg",
    }


def test_sync_tblproperties_includes_uniform_and_escapes_quotes(mocker):
    spark = mocker.MagicMock(name="spark")

    TablePropertiesSynchronizer().sync(
        spark,
        "lakehouse.bronze.demo_table",
        {"format-version": "2", "comment": "l'été"},
    )

    statement = spark.sql.call_args.args[0]
    assert statement.startswith("ALTER TABLE lakehouse.bronze.demo_table SET TBLPROPERTIES (")
    assert "format-version='2'" in statement
    assert "comment='l''été'" in statement
    assert "delta.enableIcebergCompatV2='true'" in statement
    assert "delta.universalFormat.enabledFormats='iceberg'" in statement


def test_spark_configs_use_delta_uniform_with_iceberg_rest_read_catalog():
    configs = SparkSessionFactory(_settings()).configs("lakehouse")

    assert configs["spark.sql.catalog.lakehouse"] == (
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    assert "io.delta.sql.DeltaSparkSessionExtension" in configs["spark.sql.extensions"]
    assert configs["spark.sql.catalog.iceberg_rest.type"] == "rest"
    assert (
        configs["spark.sql.catalog.iceberg_rest.uri"]
        == "http://unity-catalog:8080/api/2.1/unity-catalog/iceberg"
    )
    assert all("9083" not in value for value in configs.values())


def test_deduplicate_is_noop_without_primary_keys(mocker, sample_asset_config):
    df = mocker.MagicMock(name="dataframe")
    result = DataFrameTransformer().deduplicate(
        df,
        primary_keys=[],
        dedup_order_by=sample_asset_config.catalog_properties.dedup_order_by,
    )
    assert result is df
    df.withColumn.assert_not_called()


def test_native_dataset_reader_maps_json_and_csv(mocker, sample_asset_config):
    source = sample_asset_config.source_properties
    spark = mocker.MagicMock(name="spark")
    reader = mocker.MagicMock(name="reader")
    spark.read.options.return_value = reader
    reader.json.return_value = "json-df"

    result = NativeDatasetReader().read(
        spark, source, "s3a://landing-zone/raw/demo", {"multiLine": "true"}
    )

    assert result == "json-df"
    spark.read.options.assert_called_once_with(multiLine="true")
    reader.json.assert_called_once_with("s3a://landing-zone/raw/demo")


def test_load_service_writes_existing_table(mocker, sample_asset_config):
    spark = mocker.MagicMock(name="spark")
    spark.catalog.tableExists.return_value = True
    spark.table.return_value.count.return_value = 7

    session_provider = mocker.MagicMock()
    session_provider.create.return_value = spark
    reader = mocker.MagicMock()
    transformed = mocker.MagicMock(name="transformed")
    reader.read.return_value = "raw-df"
    transformer = mocker.MagicMock()
    transformer.transform.return_value = transformed
    transformer.deduplicate.return_value = transformed
    schema_evolution = mocker.MagicMock()
    writers = mocker.MagicMock()
    properties = mocker.MagicMock()
    snapshots = mocker.MagicMock()
    snapshots.latest.return_value = {"snapshot_id": "1"}

    service = IcebergLoadService(
        session_provider=session_provider,
        reader=reader,
        transformer=transformer,
        schema_evolution=schema_evolution,
        writers=writers,
        properties=properties,
        snapshots=snapshots,
    )

    result = service.load(sample_asset_config, "s3a://landing-zone/raw/demo")

    assert result["row_count"] == 7
    assert result["snapshot"] == {"snapshot_id": "1"}
    schema_evolution.evolve.assert_called_once()
    writers.create.assert_called_once_with(sample_asset_config.catalog_properties.write_mode)
    writers.create.return_value.write.assert_called_once()
    properties.sync.assert_called_once()


def test_load_service_creates_missing_table(mocker, sample_asset_config):
    spark = mocker.MagicMock(name="spark")
    spark.catalog.tableExists.return_value = False
    spark.table.return_value.count.return_value = 3

    session_provider = mocker.MagicMock()
    session_provider.create.return_value = spark
    reader = mocker.MagicMock()
    transformer = mocker.MagicMock()
    transformer.transform.return_value = "df"
    transformer.deduplicate.return_value = "df"
    writers = mocker.MagicMock()
    properties = mocker.MagicMock()
    snapshots = mocker.MagicMock()
    snapshots.latest.return_value = {}

    service = IcebergLoadService(
        session_provider=session_provider,
        reader=reader,
        transformer=transformer,
        schema_evolution=mocker.MagicMock(),
        writers=writers,
        properties=properties,
        snapshots=snapshots,
    )

    result = service.load(sample_asset_config, "s3a://landing-zone/raw/demo")

    assert result["table_existed"] is False
    writers.create.assert_called_once_with("overwrite")
    properties.sync.assert_not_called()


def test_snapshot_inspector_returns_latest_row(mocker):
    spark = mocker.MagicMock(name="spark")
    row = mocker.MagicMock()
    row.asDict.return_value = {"snapshot_id": 42}
    spark.sql.return_value.collect.return_value = [row]

    assert SnapshotInspector().latest(spark, "lakehouse.bronze.t") == {"snapshot_id": "42"}
