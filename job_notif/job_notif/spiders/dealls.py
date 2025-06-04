import json
import scrapy
from datetime import datetime
from scrapy.http import Response
from typing import Any, Dict, List


class DeallsSpider(scrapy.Spider):
    name = "dealls"
    allowed_domains = ["sejutacita.id"]
    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 1,
        "LOG_FILE": f"logs/dealls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    }

    def __init__(
        self,
        name: str | None = None,
        limit: int | None = None,
        required: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(name, **kwargs)
        self.name = name or self.name
        self.available_jobs = 0
        self.acquired_jobs = 0
        self.required_jobs = int(required) if required is not None else 0

        if limit is not None and required is not None:
            self.limit = max(int(limit), int(required))
        elif limit is not None and required is None:
            self.limit = int(limit) if int(limit) <= 20 else 20
        else:
            self.limit = 20

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def start(self):
        base_url = "https://api.sejutacita.id/v1/explore-job/job"
        params = {"page": 1, "limit": self.limit}
        start_url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
        yield scrapy.Request(url=start_url, callback=self.parse, meta={"page": 1})

    def parse(self, response: Response):
        if response.status != 200:
            self.logger.error(f"Failed to fetch data: {response.status}")
            return

        res: Dict = json.loads(response.text)

        available_jobs = res.get("data", {}).get("totalDocs", 0)
        if available_jobs == 0:
            self.logger.info("No jobs found.")
            return

        self.available_jobs = available_jobs
        self.required_jobs = (
            min(self.required_jobs, self.available_jobs)
            if self.required_jobs > 0
            else self.available_jobs
        )
        self.logger.info(
            f"Available jobs: {self.available_jobs}, Required jobs: {self.required_jobs}"
        )

        jobs: List[Dict] = res.get("data", {}).get("docs", [])
        if len(jobs) == 0:
            self.logger.info("Jobs data are already exhausted or not found. Stopping.")
            self.logger.info(f"Total jobs acquired: {self.acquired_jobs}")
            return

        for job in jobs:
            job_id = job.get("id")
            if not job_id:
                self.logger.warning("Job ID not found, skipping job.")
                continue

            job_title = job.get("title")
            job_category = job.get("categorySlug")
            if job_category is None or (
                job_category is not None and job_category != "it-and-engineering"
            ):
                self.logger.info(
                    f"Skipping job {job_title} due to category: {job_category}."
                )
                continue

            self.logger.info(f"Fetching details for job ID: {job_id}")
            job_slug = job.get("slug", "")
            detail_url = "https://api.sejutacita.id/v1/job-portal/job/slug/" + job_slug
            yield scrapy.Request(
                url=detail_url,
                callback=self.parse_job_details,
            )

            self.acquired_jobs += 1
            if self.acquired_jobs >= self.required_jobs:
                self.logger.info(
                    f"Required jobs acquired: {self.acquired_jobs}. Stopping."
                )
                return

        page = response.meta.get("page", 1) + 1
        next_url = f"https://api.sejutacita.id/v1/explore-job/job?page={page}&limit={self.limit}"
        self.logger.info(f"Fetching next page: {next_url}")
        yield scrapy.Request(
            url=next_url,
            callback=self.parse,
            meta={"page": page},
        )

    def parse_job_details(self, response: Response):
        if response.status != 200:
            self.logger.error(f"Failed to fetch job details: {response.status}")
            return

        res: Dict = json.loads(response.text)
        job_details = res.get("data", {}).get("result", {})
        if not job_details:
            self.logger.warning("Job details not found.")
            return

        job_item = {}
        job_id = job_details.get("id")
        if job_details.get("closed", False):
            self.logger.info(f"Job ID {job_id} is closed, skipping.")
            return
        job_item["id"] = job_id
        job_item["title"] = job_details.get("role")
        job_item["description"] = job_details.get("description")
        job_item["posted_date"] = job_details.get("publishedAt", "").split(".")[0] + "Z"
        job_item["company"] = job_details.get("company", {}).get("name")
        city = job_details.get("location", {}).get("city", {}).get("name")
        country = job_details.get("location", {}).get("country", {}).get("name")
        job_item["location"] = f"{city}, {country}" if city and country else None
        job_item["type"] = job_details.get("employmentTypes", [])
        job_item["requirement"] = job_details.get("requirements")
        salary_start = job_details.get("salaryRange", {}).get("start")
        salary_end = job_details.get("salaryRange", {}).get("end")
        job_item["salary"] = (
            f"Rp {salary_start} - {salary_end}" if salary_start and salary_end else None
        )
        job_item["remote"] = job_details.get("workplaceType") == "remote"
        job_item["source"] = self.name
        job_slug = job_details.get("slug", "")
        company_slug = job_details.get("company", {}).get("slug", "")
        job_item["url"] = f"https://dealls.com/loker/{job_slug}~{company_slug}"

        yield job_item
        self.logger.info(f"Job ID {job_id} details processed successfully.")
