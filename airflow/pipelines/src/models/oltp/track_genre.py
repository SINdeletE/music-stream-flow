from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class TrackGenre(Base):
    __tablename__ = "track_genre"

    track_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("music_oltp.track.id", ondelete="CASCADE"),
        primary_key=True,
    )
    genre_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("music_oltp.genre.id", ondelete="CASCADE"),
        primary_key=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
