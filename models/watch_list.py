from typing import Optional
from pydantic import BaseModel, Field, field_validator

class TitleQueryParams(BaseModel):
    title_type: Optional[str] = Field(
        None,
        description="`tv` or `movie`; case-insensitive",
        min_length=1,
        max_length=5
    )
    search_term: Optional[str] = Field(
        None,
        description="Keywords to match against title name",
    )
    collection_id: Optional[int] = Field(
        None,
        description="Filter by a specific collection ID",
    )

    in_watchlist: Optional[bool] = Field(
        None,
        description="Show only titles that are in / not in the watch list"
    )
    watch_status: Optional[str] = Field(
        None,
        description=(
            "Three-state title-wide watch progress filter. "
            "`unwatched`, `partially_watched`, or `fully_watched`."
        ),
        pattern="^(unwatched|partially_watched|fully_watched)$"
    )

    favourite: Optional[bool] = Field(
        None,
        description="Only titles marked as favourite for the user",
    )
    released: Optional[bool] = Field(
        None,
        description="Show only released / unreleased titles",
    )
    season_in_progress: Optional[bool] = Field(
        None,
        description=(
            "Whether to filter by seasons that are partially or fully "
            "watched (`True` → show such seasons, `False` → hide them)"
        ),
    )

    has_media_entry: Optional[bool] = Field(
        None,
        description="Only titles with (or without) any media entry",
    )

    sort_by: Optional[str] = Field(
        None,
        description=(
            "Column to sort by. "
            "`last_updated` (default), `rating`, `popularity`, "
            "`release_date`, `title_name`, `duration`, or `data_updated`"
        ),
        pattern="^(last_updated|rating|popularity|release_date|title_name|duration|data_updated)$"
    )
    direction: Optional[str] = Field(
        None,
        description="Sort direction: `ASC` or `DESC` (default DESC)",
        pattern="^(ASC|DESC|asc|desc)?$",
    )

    page: int = Field(
        1,
        ge=1,
        description="Page number (starting at 1)"
    )
    title_limit: Optional[int] = Field(
        None,
        gt=0,
        le=200,
        description="Maximum titles per page; `None` means no limit",
    )

    @field_validator("title_type")
    def _valid_title_type(cls, v: Optional[str]) -> Optional[str]:
        if v and v.lower() not in {"tv", "movie"}:
            raise ValueError("title_type must be 'tv' or 'movie'")
        return v
    