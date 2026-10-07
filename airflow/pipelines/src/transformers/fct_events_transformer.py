from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient

from src.mappers.fct_event_mapper import FctEventMapper
from src.models.dwh import (
    DimAppRelease,
    DimDevice,
    DimDeviceOs,
    DimEventType,
    DimTrack,
    FctEvent,
)
from src.models.kafka import AnalyticsEvent

from ._dimension_lookup import resolve_dimension_sks


class FctEventsTransformer:
    @staticmethod
    async def transform(
        batch: list[AnalyticsEvent],
        client: AsyncClient,
        *,
        load_id: UUID,
    ) -> list[FctEvent]:
        if not batch:
            return []

        event_names = list(dict.fromkeys(entity.event_type.value for entity in batch))
        result = await client.query(
            query=f"""
                SELECT
                    name,
                    event_type_sk
                FROM {DimEventType.table_to_insert()} FINAL
                WHERE name IN {{event_names:Array(String)}}
            """,
            parameters={"event_names": event_names},
        )
        event_type_sks = dict(result.result_rows)
        for name in event_names:
            if event_type_sks.get(name) is None:
                raise LookupError(
                    f"Dimension not found in {DimEventType.table_to_insert()}: name={name}"
                )

        device_sks = await resolve_dimension_sks(
            client,
            (entity.device_id for entity in batch),
            table_name=DimDevice.table_to_insert(),
            entity_name="device",
        )
        device_os_sks = await resolve_dimension_sks(
            client,
            (entity.device_os_id for entity in batch),
            table_name=DimDeviceOs.table_to_insert(),
            entity_name="device_os",
            version_column="valid_from",
        )
        app_release_sks = await resolve_dimension_sks(
            client,
            (entity.app_release_id for entity in batch),
            table_name=DimAppRelease.table_to_insert(),
            entity_name="app_release",
            version_column="source_created_at",
        )
        track_sks = await resolve_dimension_sks(
            client,
            (entity.track_id for entity in batch if entity.track_id is not None),
            table_name=DimTrack.table_to_insert(),
            entity_name="track",
        )

        return [
            FctEventMapper.to_dwh(
                entity,
                load_id,
                event_type_sk=event_type_sks[entity.event_type.value],
                device_sk=device_sks[entity.device_id],
                device_os_sk=device_os_sks[entity.device_os_id],
                app_release_sk=app_release_sks[entity.app_release_id],
                track_sk=track_sks[entity.track_id] if entity.track_id is not None else None,
            )
            for entity in batch
        ]
