# -*- coding: utf-8 -*-

from crawl_groceries.models import Product, BranchProduct
from crawl_groceries.database_setup import engine
from sqlalchemy.orm.session import sessionmaker


class CrawlGroceriesPipeline:

    def __init__(self):
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def process_item(self, item, spider):
        branches = item.pop('branches')
        product = Product(**item)

        self.session.add(product)
        self.session.commit()

        for branch_data in branches:
            branch_product = BranchProduct(
                product_id=product.id,
                **branch_data
            )
            self.session.add(branch_product)
            self.session.commit()
