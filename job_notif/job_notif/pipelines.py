import os
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from scrapy.spiders import Spider
from scrapy.loader import ItemLoader
from dotenv import load_dotenv, find_dotenv
from itemloaders.processors import TakeFirst, MapCompose
from .items import JobItem
from .utils import truncate_text


load_dotenv(find_dotenv())


class JobItemLoader(ItemLoader):
    default_output_processor = TakeFirst()
    posted_date_in = MapCompose(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
    description_in = MapCompose(
        lambda x: BeautifulSoup(x, "html.parser").get_text(strip=True)
    )
    requirement_in = MapCompose(
        lambda x: BeautifulSoup(x, "html.parser").get_text(strip=True),
        lambda x: x.replace(".", ".\n") if isinstance(x, str) else x,
        truncate_text,
    )


class JobPipeline:
    def process_item(self, item: dict, spider: Spider) -> JobItem:
        job_loader = JobItemLoader(item=JobItem())
        job_loader.add_value("id", item.get("id"))
        job_loader.add_value("title", item.get("title"))
        job_loader.add_value("description", item.get("description"))
        job_loader.add_value("company", item.get("company"))
        job_loader.add_value("posted_date", item.get("posted_date"))
        job_loader.add_value("expired_date", item.get("expired_date"))
        job_loader.add_value("location", item.get("location"))
        job_loader.add_value("type", item.get("type"))
        job_loader.add_value("requirement", item.get("requirement"))
        job_loader.add_value("career_level", item.get("career_level"))
        job_loader.add_value("year_experience_min", item.get("year_experience_min"))
        job_loader.add_value("year_experience_max", item.get("year_experience_max"))
        job_loader.add_value("currency", item.get("currency"))
        job_loader.add_value("salary", item.get("salary"))
        job_loader.add_value("remote", item.get("remote"))
        job_loader.add_value("source", item.get("source"))
        job_loader.add_value("url", item.get("url"))

        return job_loader.load_item()


class JobTelegramPipeline:
    def process_item(self, item: JobItem, spider: Spider) -> JobItem:
        TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
        TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
        if not TELEGRAM_CHAT_ID or not TELEGRAM_BOT_TOKEN:
            spider.logger.error("Telegram chat ID or bot token is not set.")
            return item

        bot_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        message = (
            f"*New Job Posting*\n\n"
            f"Title: {item.title}\n"
            f"Company: {item.company}\n"
            f"Location: {item.location}\n"
            f"Salary: {item.salary if item.salary else 'N/A'}\n"
            f"Posted Date: {item.posted_date.strftime('%d-%m-%Y') if item.posted_date else 'N/A'}\n"
            f"Description:\n{item.description}\n"
            f"Requirement:\n{item.requirement}\n"
            f"Source: {item.source}\n"
            f"URL: {item.url}"
        )
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
        }
        headers = {
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                bot_url,
                headers=headers,
                data=json.dumps(payload),
            )
            if response.status_code == 200:
                spider.logger.info(
                    f"Job notification sent successfully: ({item.id}) {item.title}, {item.company} - {item.location}"
                )
            else:
                spider.logger.error(
                    f"Failed to send job notification ({item.id}):\n{response.status_code} - {response.text}"
                )
        except Exception as e:
            spider.logger.error(f"Error sending job notification ({item.id}):\n{e}")
        finally:
            return item
