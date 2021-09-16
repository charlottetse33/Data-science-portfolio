from bs4 import BeautifulSoup as bs
import requests

#US reviews
US_firstpage_link = "https://www.amazon.com/KOR-Free-Hydration-Vessel-Blue/product-reviews/B001K72L9K/ref=cm_cr_arp_d_paging_btm_next_2?%27+%5C+%27ie=UTF8&reviewerType=all_reviews%27&pageNumber=1"

# headers to avoid being blocked by Amazon
headers = {
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Language' : 'en-US,en;q=0.5',
'Accept-Encoding' : 'gzip',
'DNT' : '1', # Do Not Track Request Header
'Connection' : 'close'
}

# Get review number
front_page = requests.get(US_firstpage_link, headers= headers)
front_page.encoding = 'ISO-885901'
front_page_soup = bs(front_page.text, 'html.parser')
#print(soup.prettify())
for test in front_page_soup.find_all(id = "filter-info-section"):
    front_page_text = test.get_text()
#print(front_page_text)
front_page_text_string = str(front_page_text)

for i in front_page_text_string.split():
    if i.isdigit():
        reviews_no = int(i)
#print(reviews_no)
pages_no = reviews_no // 10 + 1
print(pages_no)

#Find all reviews urls
urls = []
i = 0
while True:
    i += 1
    US_link = "https://www.amazon.com/KOR-Free-Hydration-Vessel-Blue" \
                  "/product-reviews/B001K72L9K/ref=cm_cr_getr_d_paging_btm_" \
                  "next_2?%27+%5C+%27ie=UTF8&reviewerType=all_reviews%27&page" \
                  "Number=" + str (i)
    urls.append(US_link)
    if i == pages_no:
        break

#print(urls)

US_cust_name = []
US_review_title = []
US_rate = []
US_review_content = []

for j in urls:
    US_page = requests.get(j, headers = headers)
    US_soup = bs(US_page.content, 'html.parser')

    #print(soup.prettify())

    US_names = US_soup.find_all('span',class_='a-profile-name')
    #print(US_names)
    for i in range(2,len(US_names)):
        US_cust_name.append(US_names[i].get_text())
    #print(US_cust_name)

    US_title = US_soup.find_all('a', class_='review-title-content')
    #print(US_title)

    for i in range(0,len(US_title)):
        US_review_title.append(US_title[i].get_text())
    #print(review_title)

    US_review_title[:] = [titles.strip('\n') for titles in US_review_title]
    #print(US_review_title)

    US_rating = US_soup.find_all('i', class_= 'review-rating')
    for i in range(2,len(US_rating)):
        US_rate.append(US_rating[i].get_text())
    #print(US_rate)
    #print(len(US_rate))

    US_review = US_soup.find_all("span", {"data-hook":"review-body"})
    #print(US_review)

    for i in range(0,len(US_review)):
        US_review_content.append(US_review[i].get_text())
    #print(US_review_content)

    US_review_content[:] = [reviews.strip('\n') for reviews in US_review_content]
    #print(US_review_content)
    #print(len(US_review_content))

    if len(US_cust_name) == len(US_review_title) == len(US_rate) == len(US_review_content):
        pass
    else:
        print("error")

# length are different, need to remove 2 from US_cust_name and US_rate

#debug
print(len(US_cust_name))
print(len(US_review_title))
print(len(US_rate))
print(len(US_review_content))


import pandas as pd
df = pd.DataFrame()
df['US Customer Name'] = US_cust_name
df['US Review title'] = US_review_title
df['US Ratings'] = US_rate
df['Reviews'] = US_review_content

print(df)
df.to_csv(r"C:\Users\tsetc\Downloads\US_reviews.csv", index = True)
