import requests
import json
from bs4 import BeautifulSoup

class Extractor:
    """
    Docstring for Extractor
    """
    def __init__(self):
        self.base_url = "https://api.gdeltproject.org/api/v2/doc/doc"
        self.headers = {
            "User-Agent": "CelebrityLikeabilityProject/1.0"
        }
    
    def main(self, start_date = None, end_date = None, num_records = 10, keywords = None, domain = None, country = None, language  = None):
        """
        Main method to construct query, fetch data, and scrape articles.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.num_records = num_records
        self.keywords = keywords
        self.domain = domain
        self.country = country
        self.language = language
        self.article_count = 0

        if num_records > 250:
            self.multiple_handler()
        
        data = self.fetch_data(self.construct_query())
        self.scrape_articles(data)
        data = self.add_tags(data, self.keywords)
        return data
    
    def multiple_handler(self):
        """
        Handles cases where the number of records requested exceeds limit by repeating requests.
        """
        if self.num_records > 250:
            times = self.num_records // 250
            remainder = self.num_records % 250
            self.num_records = 250
            all_data = {"articles": []}
            for _ in range(times):
                data = self.fetch_data(self.construct_query())
                self.scrape_articles(data)
                all_data["articles"].extend(data["articles"])
            if remainder > 0:
                self.num_records = remainder
                data = self.fetch_data(self.construct_query())
                self.scrape_articles(data)
                all_data["articles"].extend(data["articles"])
            return all_data
        else:
            return None
    
    def construct_query(self) -> str:
        """
        Constructs the query URL with given parameters.
        """
        if len(self.keywords) > 1:
            query = "("
            for keyword in self.keywords:
                query += f"\"{keyword}\" OR "
            query = query.rstrip(" OR ") + ")"
        else:
            query = f"\"{self.keywords[0]}\""
        query = requests.utils.quote(query)
        if self.start_date:
            query += f"&startdatetime={self.start_date}000000"
        if self.end_date:
            query += f"&enddatetime={self.end_date}235959"
        if self.num_records:
            query += f"&maxrecords={self.num_records}"
        if self.domain:
            query += f"&domain={self.domain}"
        if self.country:
            query += f"&country={self.country}"
        if self.language:
            query += f"&language={self.language}"
        query += "&format=json"
        return f"{self.base_url}?query={query}"

    def fetch_data(self, query_url: str) -> dict:
        """
        Fetches data from the constructed query URL.
        """
        response = requests.get(query_url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()
    
    def scrape_article(self, url: str) -> str:
        """
        Scrapes the article content from the given URL.
        """
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'lxml')
            paragraphs = soup.find_all('p')
            article_text = ' '.join([para.get_text() for para in paragraphs])
        else:
            response.raise_for_status()
        return article_text
    
    def scrape_articles(self, response: dict) -> list:
        """
        Scrapes articles from the response data.
        """
        for index, item in enumerate(response['articles']):
            self.article_count += 1
            url = item.get('url')
            if url:
                try:
                    article_text = self.scrape_article(url)
                    response['articles'][index]['text'] = article_text
                    print(f"Scraped article {self.article_count}: {url}")
                except Exception as e:
                    print(f"Failed to scrape article {self.article_count}: {url}. Error: {e}")
                    response['articles'][index]['text'] = ""

    def add_tags(self, data: dict, tags: list) -> dict:
        """
        Adds tags to each article in the data.
        """
        for index in range(len(data['articles'])):
            data['articles'][index]['tags'] = tags
        return data

if __name__ == "__main__":
    extractor = Extractor()

    data = extractor.main(
        start_date="20230101",
        end_date="20251231",
        keywords=["Brad Pitt"],
        num_records=10
    )
    with open("extracted_news.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)