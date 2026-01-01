from transformers import pipeline
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy import text

class SentimentAnalyzer:
    def __init__(self, celebrity_name = None, table_name="celebrity_data"):
        """
        Initialize the SentimentAnalyzer with a pre-trained sentiment analysis model.
        """
        self.celebrity_name = celebrity_name
        self.input_table_name = table_name
        self.hugging_face_transformer = pipeline("text-classification", model="tabularisai/multilingual-sentiment-analysis")
        try:
            nltk.data.find('sentiment/vader_lexicon.zip')
        except Exception as e:
            nltk.download('vader_lexicon')
        self.vader_analyzer = SentimentIntensityAnalyzer()

        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        user = "celebrities_user"
        password = POSTGRES_PASSWORD
        host = "localhost"
        port = "5432"
        db = "celebrities_db"
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
        if self.celebrity_name:
            query = f"""
            SELECT *
            FROM {self.input_table_name}
            WHERE tags ILIKE '%%{self.celebrity_name}%%';
            """
        else:
            query = f"SELECT * FROM {self.input_table_name};"
        self.df = pd.read_sql(query, con=self.engine)
    
    def driver(self):
        """
        Driver method to analyze sentiment for each article in the DataFrame.
        """
        new_df_scores = []
        for index, row in self.df.iterrows():
            article_text = row["text"]
            sentiment_scores = []
            if len(article_text) > 512:
                text_list = self.chunk_text(article_text)
                for text_chunk in text_list:
                    score = self.analyze_sentiment(text_chunk)
                    sentiment_scores.append(score)
            average_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
            new_df_scores.append(average_sentiment)
        self.df["sentiment_score"] = new_df_scores
            
    def chunk_text(self, text, chunk_size=512):
        """
        Chunk the text into smaller pieces for analysis.
        """
        text_list = []
        for i in range(0, len(text), chunk_size):
            text_list.append(text[i:i+chunk_size])
        return text_list

        
    def analyze_sentiment(self, text):
        """
        Analyze the sentiment of the given text.
        """
        hugging_face_result = self.hugging_face_transformer(text)
        vader_result = self.vader_analyzer.polarity_scores(text)['compound']
        textblob_result = TextBlob(text).sentiment.polarity
        # hugging face is the only one we need to manipulate
        hugging_face_sentiment_map = {"Very Negative": -2, "Negative": -1, "Neutral": 0, "Positive": 1, "Very Positive": 2}
        if hugging_face_sentiment_map[hugging_face_result[0]['label']] < 0:
            hugging_face_result = hugging_face_result[0]['score'] * -1
        else:
            hugging_face_result = hugging_face_result[0]['score']
        
        # ensemble the results with more weight to hugging face and vader since textblob uses less percision
        final_score = (hugging_face_result * 5 + vader_result * 4 + textblob_result) / 10
        return final_score
        
    def write_to_db(self, table_name="celebrity_sentiment_data"):
        """
        Write the DataFrame with sentiment scores back to the PostgreSQL database.
        """
        try:
            self.df.to_sql(table_name, self.engine, if_exists='append', index=False)
            print(f"Sentiment data written to table {table_name} successfully.")
        except Exception as e:
            print(f"Error writing sentiment data to table {table_name}: {e}")
        
if __name__ == "__main__":
    analyzer = SentimentAnalyzer("Bradley Cooper", "bradley_cooper_data")
    analyzer.driver()
    analyzer.write_to_db()