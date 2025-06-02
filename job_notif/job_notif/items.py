from typing import Optional
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class JobItem:
    id: Optional[str] = field(default=None)
    title: Optional[str] = field(default=None)
    description: Optional[str] = field(default=None)
    company: Optional[str] = field(default=None)
    posted_date: Optional[datetime] = field(default=None)
    expired_date: Optional[datetime] = field(default=None)
    location: Optional[str] = field(default=None)
    type: Optional[str] = field(default=None)
    requirement: Optional[str] = field(default=None)
    career_level: Optional[str] = field(default=None)
    year_experience_min: Optional[int] = field(default=None)
    year_experience_max: Optional[int] = field(default=None)
    currency: Optional[str] = field(default=None)
    salary: Optional[str] = field(default=None)
    remote: Optional[bool] = field(default=None)
    source: Optional[str] = field(default=None)
    url: Optional[str] = field(default=None)
