CREATE OR REPLACE TABLE
  `final-project-vinazol.tweets_dataset.F_dataset_flood` AS
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_trivandrum`
UNION ALL 
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_kollam`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_alappuzha`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_pathanamthitta`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_kottayam`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_idukki`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_ernakulam`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_thrissur`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_palakkad`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_malappuram`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_kozhikode`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_wayanad`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_kannur`
UNION ALL
SELECT tweet_id, username, tweet_text, retweets, favs, district FROM `final-project-vinazol.tweets_dataset.D_dataset_kasargod`