import scrapy
import json
import random

UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

class TikiApiSpider(scrapy.Spider):
    name = 'tiki_api'

    def start_requests(self):
        url = 'https://tiki.vn/api/v2/products?limit=40&include=advertisement&aggregations=2&trackity_id=c9f14780-883a-e7c5-d7a3-f912014de4bb&q=gi%C3%A0y+th%E1%BB%83+thao+nam&page=1'
        headers = {
            'User-Agent': random.choice(UA_LIST),
        }
        yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):
        data = json.loads(response.text)
        for product in data.get('data', []):
            yield {
                'product_id': product.get('id'),
                'name': product.get('name'),
                'brand_name': product.get('brand_name'),
                'price': product.get('price'),
                'original_price': product.get('original_price'),
                'available': product.get('availability '),
                'quantity_sold': product.get('quantity_sold.value'),
                'rating_average': product.get('rating_average'),
                'review_count': product.get('review_count'),
            }

        paging = data.get('paging', {})
        current_page = paging.get('current_page', 1)
        total = paging.get("total", 0)
        pages = (total // 40) + 1
        if current_page < pages:
            next_page = current_page + 1
            next_url = f"https://tiki.vn/api/v2/products?limit=40&include=advertisement&aggregations=2&trackity_id=c9f14780-883a-e7c5-d7a3-f912014de4bb&q=gi%C3%A0y+th%E1%BB%83+thao+nam&page={next_page}"
            headers = {"User-Agent": random.choice(UA_LIST)}
            yield scrapy.Request(next_url, headers=headers, callback=self.parse)