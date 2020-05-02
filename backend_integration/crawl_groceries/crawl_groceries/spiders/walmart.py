from scrapy import Spider, Request
import json


class WalmartSpider(Spider):
    name = 'walmart'

    start_urls = [
        'https://www.walmart.ca/en/grocery/N-117'
    ]

    allowed_branches = [3124, 3106]
    walmart_domain_image = 'https://i5.walmartimages.ca/'

    def parse(self, response):

        category_link = response.css(
            '.tileGenV2_wrapper .tile a::attr(href)'
        ).getall()

        yield from response.follow_all(
            category_link, self.parse_category
        )

    def parse_category(self, response):

        product_url = response.css(
            '.shelf-thumbs article.dynamic-cart-thumb a::attr(href)'
        ).getall()

        yield from response.follow_all(
            product_url, self.parse_product
        )

        # Product pagination
        pagination = response.css('#loadmore::attr(href)').get()
        yield response.follow(
            f'https://www.walmart.ca/{pagination}', self.parse_category
        )

    def parse_product(self, response):

        page_script = response.css(
            'script::text')[10].get().replace(
            'window.__PRELOADED_STATE__=', '').replace(';', '')

        json_data = json.loads(page_script)
        sku_id = json_data['product']['activeSkuId']

        upc = json_data['entities']['skus'][sku_id]['upc'][0]
        image = ','.join(
            [f'{self.walmart_domain_image}{image["large"]["url"]}'
             for image in json_data['entities']['skus'][sku_id]['images']]
        )

        categories = json_data['product']['item'][
            'primaryCategories'][0]['hierarchy']

        categories = [category['displayName']['en'] for category in categories]
        categories.reverse()
        product = {
            'store': 'Walmart',
            'barcodes': upc,
            'sku': json_data['product']['activeSkuId'],
            'brand': json_data['entities']['skus'][sku_id]['brand']['name'],
            'name': json_data['product']['item']['name']['en'],
            'description': json_data['entities']['skus'][sku_id]['longDescription'],
            'package': json_data['product']['item']['description'],
            'image_urls': image,
            'category': '|'.join(categories),
            'product_url': response.url
        }

        
        # Get branch 3124
        request = Request(
            url='https://www.walmart.ca/api/product-page/find-in-store?'
            f'latitude=48.4120872&longitude=-89.2413988&lang=en&upc={upc}',
            cb_kwargs=product,
            meta={'upc': upc},
            callback=self.branch_3106
        )

        yield request
        
    def branch_3106(self, response, **kwargs):
        item = response.text
        upc = response.meta['upc']
        # Get branch 3106
        yield Request(
                url='https://www.walmart.ca/api/product-page/find-in-store?'
                f'latitude=43.6562242&longitude=-79.4355773&lang=en&upc={upc}',
                cb_kwargs=kwargs,
                meta={'item':item},
                callback=self.parse_branch
        )

    def parse_branch(self, response, **kwargs):
        
        json_branch_3106 = json.loads(response.text)
        json_branch_3124 = json.loads(response.meta['item'])
        branches = []

        for branch in json_branch_3106['info'] + json_branch_3124['info']:
            if branch['id'] in self.allowed_branches:
                branches.append({
                    'branch': branch['id'],
                    'price': branch.get('sellPrice', 0),
                    'stock': branch['availableToSellQty']
                })

        yield {**kwargs, 'branches': branches}
