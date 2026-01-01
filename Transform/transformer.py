import pandas as pd
import unicodedata
import numpy as np
from Extract.news_extractor import Extractor

class Transformer:
    """
    A class to transform and clean extracted news data.
    """
    def clean_data(self, data: dict) -> pd.DataFrame:
        """
        Cleans the extracted news data by removing duplicates and handling missing values.
        """
        records = data["articles"]
        df = pd.DataFrame(records)

        # Remove duplicates based on 'url'
        df.drop_duplicates(subset=['url'], inplace=True)

        # Handle missing values: drop rows with missing 'title', 'url' or 'text'
        df["title"] = df["title"].replace("", np.nan)
        df["url"] = df["url"].replace("", np.nan)
        df["text"] = df["text"].replace("", np.nan)
        df.dropna(subset=['title', 'url', 'text'], inplace=True)

        # Clean article text
        df['text'] = df['text'].apply(self.clean_article_text)
        # fill url_mobile missing values with url
        df['url_mobile'] = df['url_mobile'].replace("", np.nan).fillna(df['url'])
        df["seendate"] = df["seendate"].replace("", np.nan).bfill().ffill()
        df["socialimage"] = df["socialimage"].replace("", np.nan).fillna("Unknown")
        # fill domain missing values by url's domain
        df["domain"] = df['domain'].replace("", np.nan).fillna(df['url'].apply(self.calc_domain))
        df["language"] = df["language"].replace("", np.nan).fillna("Unknown")
        df["sourcecountry"] = df["sourcecountry"].replace("", np.nan).fillna("Unknown")

        df = self.clean_duplicates(df)

        return df
    
    def clean_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Look for matches by title and remove duplicates
        """
        df['title'] = df['title'].str.lower()
        df.drop_duplicates(subset=['title'], inplace=True)
        return df

    def calc_domain(self, url: str) -> str:
        """
        Extracts the domain from a given URL.
        """
        domain = url.split("/")[2]
        return domain

    def clean_article_text(self, text: str) -> str:
        """
        Cleans the article text by removing extra whitespace and normalizing unicode characters.
        """
        # Remove extra whitespace
        text = ' '.join(text.split())
        # Normalize unicode characters
        text = unicodedata.normalize("NFKC", text)
        text = text.lower()
        return text

if __name__ == "__main__":
    extract = Extractor()
    data = extract.main(
        start_date="20230101",
        end_date="20231231",
        keywords=["Jonah Hill"],
        num_records=10
    )
    transformer = Transformer()
    cleaned_df = transformer.clean_data(data)
    cleaned_df.to_csv("cleaned_news.csv", index=False, encoding='utf-8')