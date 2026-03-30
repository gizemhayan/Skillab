"""
Abstract Base Scraper — Scalability Contract for Skillab Turkey.

Defines the interface that every platform-specific scraper must implement.
Adding support for LinkedIn, Indeed, or any other job portal requires only
creating a new class that inherits from BaseScraper and implementing the
three abstract methods below — no changes to the analysis pipeline needed.

Design pattern: Template Method via ABC.
Dependency rule: All code in the pipeline depends on this abstraction,
                 never on concrete scraper implementations.

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from src.utils.logger import get_logger
from src.utils.models import JobDetail, JobListing


class BaseScraper(ABC):
    """
    Abstract base class enforcing the scraper contract across all platforms.

    Concrete implementations (KariyerScraper, LinkedInScraper, IndeedScraper)
    must implement every abstract method. The context-manager protocol is
    provided here so all scrapers automatically support `with` blocks.

    Class Attributes:
        PLATFORM_NAME: Human-readable name of the target platform.
                       Used in logging and model metadata.
        BASE_URL:      Root URL for the platform's job search endpoint.
    """

    PLATFORM_NAME: str = ""
    BASE_URL: str = ""

    def __init__(self) -> None:
        self.logger = get_logger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )

    # -----------------------------------------------------------------------
    # Abstract Interface
    # -----------------------------------------------------------------------

    @abstractmethod
    def fetch_jobs(self, keyword: str, page_count: Optional[int] = 1) -> List[JobListing]:
        """
        Retrieve job listing cards from the search results pages.

        Args:
            keyword:    Search term (e.g. "veri analisti", "python developer").
            page_count: Number of paginated result pages to traverse.
                        If None, scraper should continue until the last page.

        Returns:
            Ordered list of JobListing model instances. May be empty if the
            platform returns no results or all requests fail.
        """
        ...

    @abstractmethod
    def fetch_job_detail(self, listing: JobListing) -> JobDetail:
        """
        Retrieve and parse the full job description from a detail page.

        Args:
            listing: A JobListing previously returned by fetch_jobs().

        Returns:
            JobDetail enriched with description, structured fields, and
            raw skill tokens. On parse failure, description should be set
            to an empty string and required_skills_raw to an empty list —
            never raise an unhandled exception from this method.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """
        Release all held resources (HTTP sessions, browser drivers, etc.).

        Must be idempotent — calling close() multiple times must not raise.
        """
        ...

    # -----------------------------------------------------------------------
    # Context Manager Protocol  (inherited by all subclasses automatically)
    # -----------------------------------------------------------------------

    def __enter__(self) -> "BaseScraper":
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> bool:
        self.close()
        return False  # never suppress exceptions
