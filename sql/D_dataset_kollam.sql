ALTER TABLE
  `final-project-vinazol.staging_tweets_dataset.dataset_flood` ADD COLUMN
IF NOT EXISTS district STRING;
CREATE OR REPLACE TABLE
  `final-project-vinazol.tweets_dataset.D_dataset_kollam` AS
SELECT
  tweet_id,
  username,
  tweet_text,
  retweets,
  favs,
  IFNULL(district,
    'Kollam') AS district
FROM
  `final-project-vinazol.staging_tweets_dataset.dataset_flood`
WHERE `tweet_text` LIKE '%kollam%';
