import requests
import csv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


def fetch_archive_data(url, year, month):
    response = requests.get(url)
    if response.status_code == 200:
        json_response = response.json()
        return json_response["response"]["docs"]
    else:
        print("Failed to fetch data")
        return None


def response_to_dataframe(data):
    columns = ["headline", "pub_date", "web_url", "nyt_id",
               "keyword1", "keyword2", "keyword3", "keyword4", "keyword5"]

    article_data = []
    for article in data:
        if "headline" not in article:
            continue
        keyword1 = article["keywords"][0]["value"] if len(
            article["keywords"]) > 0 else ""
        keyword2 = article["keywords"][1]["value"] if len(
            article["keywords"]) > 1 else ""
        keyword3 = article["keywords"][2]["value"] if len(
            article["keywords"]) > 2 else ""
        keyword4 = article["keywords"][3]["value"] if len(
            article["keywords"]) > 3 else ""
        keyword5 = article["keywords"][4]["value"] if len(
            article["keywords"]) > 4 else ""
        article_data.append([article["headline"]["main"], article["pub_date"], article["web_url"],
                            article["_id"], keyword1, keyword2, keyword3, keyword4, keyword5])

    df = pd.DataFrame(columns=columns, data=article_data)
    return df


def load_keywords_to_postgres(df):
    conn = psycopg2.connect(
        "dbname='destination_db' user='postgres' host='destination_db' password='secret'")
    cur = conn.cursor()

    keywords = pd.concat(
        [df["keyword1"], df["keyword2"], df["keyword3"], df["keyword4"], df["keyword5"]]).tolist()

    unique_keywords = set(keywords)

    # update this to run in a batch instead of a loop
    for keyword in unique_keywords:
        cur.execute(
            "INSERT INTO keywords (keyword) VALUES (%s) ON CONFLICT (keyword) DO NOTHING",
            (keyword,))

    conn.commit()
    cur.close()
    conn.close()


def load_articles_to_postgress(df):
    conn = psycopg2.connect(
        "dbname='destination_db' user='postgres' host='destination_db' password='secret'")
    cur = conn.cursor()
    # Retrieve keyword IDs
    cur.execute("SELECT keyword, keyword_id FROM keywords;")
    keyword_to_id = {keyword: keyword_id for keyword,
                     keyword_id in cur.fetchall()}

    # Map article keywords to keyword IDs
    df['keyword1_id'] = df['keyword1'].map(keyword_to_id)
    df['keyword2_id'] = df['keyword2'].map(keyword_to_id)
    df['keyword3_id'] = df['keyword3'].map(keyword_to_id)
    df['keyword4_id'] = df['keyword4'].map(keyword_to_id)
    df['keyword5_id'] = df['keyword5'].map(keyword_to_id)

    # Insert articles into the article table
    insert_articles_sql = "INSERT INTO articles (headline, pub_date, web_url, nyt_id, keyword1_id, keyword2_id, keyword3_id, keyword4_id, keyword5_id ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    article_data = df[['headline', 'pub_date', 'web_url', 'nyt_id',
                       'keyword1_id', 'keyword2_id', 'keyword3_id', 'keyword4_id', 'keyword5_id']].values.tolist()
    execute_batch(cur, insert_articles_sql, article_data)

    # Commit the transaction to insert articles
    conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()


def main():
    years = [2023, 2022, 2021]
    for year in years:
        url = f"https://api.nytimes.com/svc/archive/v1/{year}/1.json?api-key=jIkUBgtnjtDAEeEBVh7b5FepqGGuoVXa"
        response_data = fetch_archive_data(url, year, 1)
        # TODO: check if the data fetch failed
        response_dataframe = response_to_dataframe(response_data)
        print(response_dataframe.iloc[0])
        load_keywords_to_postgres(response_dataframe)
        load_articles_to_postgress(response_dataframe)


if __name__ == "__main__":
    main()
