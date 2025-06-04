import uuid
import json
import scrapy
from datetime import datetime
from scrapy.http import Response
from typing import Any, Dict, List


class JobstreetSpider(scrapy.Spider):
    name = "jobstreet"
    allowed_domains = ["jobsearch-api-ts.cloud.seek.com.au", "id.jobstreet.com"]
    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "CONCURRENT_REQUESTS": 1,
        "LOG_FILE": f"logs/jobstreet_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
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
            self.limit = int(limit)
        else:
            self.limit = 100

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def start(self):
        base_url = "https://jobsearch-api-ts.cloud.seek.com.au/v5/search"
        params = {
            "siteKey": "ID-Main",
            "pageSize": self.limit,
            "page": 1,
            "sortMode": "ListedDate",
        }
        start_url = f"{base_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
        yield scrapy.Request(url=start_url, callback=self.parse, meta={"page": 1})

    def parse(self, response: Response):
        if response.status != 200:
            self.logger.error(f"Failed to fetch data: {response.status}")
            return

        res: Dict = json.loads(response.text)

        available_jobs = res.get("totalCount", 0)
        if available_jobs == 0:
            self.logger.info("No jobs found.")
            return

        self.available_jobs = available_jobs
        self.required_jobs = (
            min(self.required_jobs, self.available_jobs)
            if self.required_jobs > 0
            else self.available_jobs
        )
        self.logger.info(f"Total jobs available: {self.available_jobs}")

        jobs: List[Dict] = res.get("data", [])
        if len(jobs) == 0:
            self.logger.info("Jobs data are already exhausted or not found. Stopping.")
            self.logger.info(f"Total jobs acquired: {self.acquired_jobs}")
            return

        for job in jobs:
            job_id = job.get("id")
            if not job_id:
                self.logger.warning("Job ID not found, skipping job.")
                continue

            job_title = job.get("title", "")
            classification = (
                job.get("classifications", [])[0]
                if job.get("classifications")
                else None
            )
            classification_desc = (
                classification.get("classification", {}).get("description")
                if classification
                else None
            )
            if classification_desc is None or (
                classification_desc is not None
                and classification_desc != "Information & Communication Technology"
            ):
                self.logger.info(
                    f"Skipping job {job_title} due to classification: {classification_desc}"
                )
                continue

            self.logger.info(f"Fetching details for job ID: {job_id}")
            posted_date = job.get("listingDate")
            viewed_correlation_id = str(uuid.uuid4())
            session_id = str(uuid.uuid4())
            payload = {
                "operationName": "GetJobDetails",
                "variables": {
                    "jobId": job_id,
                    "jobDetailsViewedCorrelationId": viewed_correlation_id,
                    "sessionId": session_id,
                    "locale": "en-ID",
                },
                "query": 'query GetJobDetails($jobId: ID!, $jobDetailsViewedCorrelationId: String!, $sessionId: String!, $locale: Locale!) { jobDetails( id: $jobId tracking: {channel: "WEB", jobDetailsViewedCorrelationId: $jobDetailsViewedCorrelationId, sessionId: $sessionId} ) { job { id title abstract content(platform: WEB) advertiser { name(locale: $locale) } location { label(locale: $locale, type: LONG) } workTypes { label(locale: $locale) } salary { label } isExpired } } }',
            }
            yield scrapy.Request(
                url="https://id.jobstreet.com/graphql",
                method="POST",
                headers=self.headers,
                body=json.dumps(payload),
                callback=self.parse_job_details,
                meta={"job_id": job_id, "posted_date": posted_date},
            )

            self.acquired_jobs += 1
            if self.acquired_jobs >= self.required_jobs:
                self.logger.info(
                    f"Required jobs limit reached: {self.required_jobs}. Stopping further processing."
                )
                return

        page = response.meta.get("page", 1) + 1
        next_url = (
            response.request.url.replace(f"page={page - 1}", f"page={page}")
            if response.request
            else None
        )
        if next_url:
            self.logger.info(f"Fetching next page: {page}")
            yield scrapy.Request(url=next_url, callback=self.parse, meta={"page": page})

    def parse_job_details(self, response: Response):
        if response.status != 200:
            self.logger.error(f"Failed to fetch job details: {response.status}")
            return

        res: Dict = json.loads(response.text)
        job_details: Dict = res.get("data", {}).get("jobDetails", {}).get("job", {})
        if not job_details:
            self.logger.warning("Job details not found in response.")
            return

        job_item = {}
        job_id = response.meta.get("job_id")
        if job_details.get("isExpired", False):
            self.logger.info(f"Job ID {job_id} is expired, skipping.")
            return
        job_item["id"] = job_id
        job_item["posted_date"] = response.meta.get("posted_date")
        job_item["title"] = job_details.get("title")
        job_item["description"] = job_details.get("abstract")
        job_item["company"] = job_details.get("advertiser", {}).get("name", "")
        job_item["location"] = job_details.get("location", {}).get("label", "")
        job_item["type"] = job_details.get("workTypes", {}).get("label", "")
        job_item["requirement"] = job_details.get("content")
        job_item["salary"] = job_details.get("salary", {}).get("label", "")
        job_item["source"] = self.name
        job_item["url"] = f"https://id.jobstreet.com/id/job/{job_id}"

        yield job_item
        self.logger.info(f"Job ID {job_id} details fetched successfully.")
