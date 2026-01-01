from Extract.news_extractor import Extractor
from Transform.transformer import Transformer
from Load.loader import Load
import os

def main(start_date = None, end_date = None, num_records = 100, keywords = None, domain = None, country = None, language  = None, table_name="celebrity_data"):
    """
    Main driver method to run the ETL process
    """
    # Initialize classes
    extractor = Extractor()
    transformer = Transformer()
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    loader = Load("celebrities_user", POSTGRES_PASSWORD, "localhost", "5432", "celebrities_db")

    # Extract
    raw_data = extractor.main(start_date, end_date, num_records, keywords, domain, country, language)

    # Transform
    transformed_data = transformer.clean_data(raw_data)

    # Load
    loader.write_data(transformed_data, table_name)
    print("ETL process completed successfully.")

if __name__ == "__main__":
    main(
        start_date="20250101",
        end_date="20251231",
        keywords=["Bradley Cooper"],
        num_records=100,
        table_name="bradley_cooper_data"
    )
    main(
        start_date="20240101",
        end_date="20241231",
        keywords=["Bradley Cooper"],
        num_records=100,
        table_name="bradley_cooper_data"
    )
    main(
        start_date="20230101",
        end_date="20231231",
        keywords=["Bradley Cooper"],
        num_records=100,
        table_name="bradley_cooper_data"
    )
    main(
        start_date="20220101",
        end_date="20221231",
        keywords=["Bradley Cooper"],
        num_records=100,
        table_name="bradley_cooper_data"
    )
    main(
        start_date="20210101",
        end_date="20211231",
        keywords=["Bradley Cooper"],
        num_records=100,
        table_name="bradley_cooper_data"
    )

