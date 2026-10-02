from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, func
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class TrackRelease(Base):
    __tablename__ = "track_release"
    __table_args__ = (
        CheckConstraint(
            "track_number > 0", name="ck_track_release_track_number_positive"
        ),
        CheckConstraint(
            "disc_number IS NULL OR disc_number > 0",
            name="ck_track_release_disc_number_positive_or_null",
        ),
    )

    track_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("music_oltp.track.id", ondelete="CASCADE"),
        primary_key=True,
    )
    release_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("music_oltp.release.id", ondelete="CASCADE"),
        primary_key=True,
    )
    track_number: Mapped[int] = mapped_column(Integer)
    disc_number: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
