import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

from celery import Celery, Task

app = Celery('tender_tasks', broker='redis://localhost:6379/0')
app.conf.update(
    task_always_eager=True,
)


class FetchPageTask(Task):
    def run(self, page_number):
        url = f"https://zakupki.gov.ru/epz/order/extendedsearch/results.html?fz44=on&pageNumber={page_number}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            print(f"Ошибка при получении страницы {page_number}: {e}")
            return

        soup = BeautifulSoup(response.text, "html.parser")
        tender_links = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "printForm/view.html?regNumber=" in href:
                if not href.startswith("http"):
                    href = "https://zakupki.gov.ru" + href
                tender_links.add(href)

        for print_link in tender_links:
            xml_link = print_link.replace("view.html", "viewXml.html")
            parse_xml_task.delay(print_link, xml_link)

        return f"Страница {page_number} обработана, найдено {len(tender_links)} тендеров."


@app.task(base=FetchPageTask, name="fetch_page")
def fetch_page_task(page_number):
    return FetchPageTask().run(page_number)


# задача для парсинга xml формы
class ParseXMLTask(Task):
    def run(self, print_link, xml_link):
        try:
            response = requests.get(xml_link)
            response.raise_for_status()
        except Exception as e:
            print(f"Ошибка получения XML для {xml_link}: {e}")
            pub_date = None
            print(f"{print_link} - {pub_date}")
            return pub_date

        try:
            root = ET.fromstring(response.content)
            elem = root.find(".//publishDTInEIS")
            pub_date = elem.text if elem is not None else None
        except Exception as e:
            print(f"Ошибка парсинга XML для {xml_link}: {e}")
            pub_date = None


        print(f"{print_link} - {pub_date}")
        return pub_date


# регистрирация задачи для парсинга xml.
@app.task(base=ParseXMLTask, name="parse_xml")
def parse_xml_task(print_link, xml_link):
    return ParseXMLTask().run(print_link, xml_link)


if __name__ == "__main__":
    for page in [1, 2]:
        result = fetch_page_task.delay(page)
        print(result.get())