### Problem Set 2- Webscraping
For this assignment, I didn't end up using the data I scraped because there wasn't enough and it was too inconsistent. I started by scraping Alltrails.com, but it was extremely difficult to scrape because they dynamically loaded their data onto the page as you scrolled, which meant I needed to automate my browser to get more data instead of just making individual requests. I was able to automate by browser using phantomjs and get a few hundred hikes into my database, but eventually got locked out with captchas. 

Next, I tried scraping goodreads.com. This was more successful, as you can get to a book's page by going to goodreads.com/book/show/id, where id is a 7 digit number. I was able to get about 50 or so books into my database before goodreads blocked my ip address. I also tried with multiple proxies, but got blocked on those after a while too. Finally, I tried IMDB, which also blocked my ip address after a few hundred requests.

I turned to kaggle for some big datasets, and found a dataset with direct links to goodreads books. If you access a goodreads page with their unique book title/id in the link, you can avoid the redirect from a standard goodreads page to a goodreads page with the book's unique id. I used this dataset to get about 9000 books (code in scraper.js).
![books](https://raw.githubusercontent.com/gkgkgkgk/ECE464-Databases/main/Problem%20Set%202/imgs/books.png)

I found another dataset that had data from kickstarter, so I downloaded that dataset and used it to populate a database with about 375k kickstarter projects (code for that found in kickstarterscraper.js). Finally, I indexed my database by the pledged amount, so I could quickly search for projects based on the amount of money they raised.
![kicks](https://raw.githubusercontent.com/gkgkgkgk/ECE464-Databases/main/Problem%20Set%202/imgs/kicks.png)

Finally, I ran some queries on my kickstarter database to see some interesting metrics, including the kickstarter project with the most funding, kickstarters that made a lot of money but still failed, and more (code in query.js).
