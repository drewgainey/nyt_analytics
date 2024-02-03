CREATE TABLE keywords (
   keyword_id SERIAL PRIMARY KEY, 
   keyword TEXT UNIQUE
);

CREATE TABLE articles (
    article_id SERIAL PRIMARY KEY,
    headline TEXT,
    pub_date DATE,
    web_url TEXT,
    nyt_id TEXT,
    keyword1_id INT,
    keyword2_id INT,
    keyword3_id INT,
    keyword4_id INT,
    keyword5_id INT
);