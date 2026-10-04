from uuid import UUID

from src.models.dwh import BrgArtistRelease
from src.models.oltp import ArtistRelease


class BrgArtistReleaseMapper:
    @staticmethod
    def to_dwh(
        entity: ArtistRelease,
        load_id: UUID,
        *,
        artist_sk: int,
        release_sk: int,
    ) -> BrgArtistRelease:
        if artist_sk is None:
            raise ValueError("artist_sk must be resolved before mapping a bridge")
        if release_sk is None:
            raise ValueError("release_sk must be resolved before mapping a bridge")

        return BrgArtistRelease(
            artist_sk=artist_sk,
            release_sk=release_sk,
            artist_number=entity.artist_number,
            load_id=load_id,
        )
