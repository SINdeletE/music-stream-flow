from collections.abc import Iterable
from uuid import UUID

from clickhouse_connect.driver.asyncclient import AsyncClient


async def lookup_dimension_sks(
    client: AsyncClient,
    entity_ids: Iterable[UUID],
    *,
    table_name: str,
    entity_name: str,
    version_column: str = "updated_at",
) -> dict[UUID, int | None]:
    entity_sks: dict[UUID, int | None] = {}
    for entity_id in entity_ids:
        entity_sks[entity_id] = None

    if not entity_sks:
        return {}

    result = await client.query(
        query=f"""
            SELECT
                {entity_name}_id,
                argMax({entity_name}_sk, ({version_column}, {entity_name}_sk))
                    AS {entity_name}_sk
            FROM {table_name}
            WHERE {entity_name}_id IN {{entity_ids:Array(UUID)}}
            GROUP BY {entity_name}_id
        """,
        parameters={"entity_ids": list(entity_sks)},
    )

    for entity_id, entity_sk in result.result_rows:
        entity_sks[entity_id] = entity_sk

    return entity_sks


async def resolve_dimension_sks(
    client: AsyncClient,
    entity_ids: Iterable[UUID],
    *,
    table_name: str,
    entity_name: str,
    version_column: str = "updated_at",
) -> dict[UUID, int]:
    entity_sks = await lookup_dimension_sks(
        client,
        entity_ids,
        table_name=table_name,
        entity_name=entity_name,
        version_column=version_column,
    )

    resolved_sks: dict[UUID, int] = {}
    for entity_id, entity_sk in entity_sks.items():
        if entity_sk is None:
            raise LookupError(
                f"Dimension not found in {table_name}: {entity_name}_id={entity_id}"
            )
        resolved_sks[entity_id] = entity_sk

    return resolved_sks
