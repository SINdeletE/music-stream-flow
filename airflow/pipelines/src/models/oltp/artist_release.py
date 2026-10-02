from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ArtistRelease(Base):
    __tablename__ = "artist_release"
    __table_args__ = (
        CheckConstraint(
            "artist_number > 0", name="ck_artist_release_artist_number_positive"
        ),
    )

    artist_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("music_oltp.artist.id", ondelete="CASCADE"),
        primary_key=True,
    )
    release_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("music_oltp.release.id", ondelete="CASCADE"),
        primary_key=True,
    )
    artist_number: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
