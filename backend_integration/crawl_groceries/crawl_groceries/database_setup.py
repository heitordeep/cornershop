from sqlalchemy import create_engine

from crawl_groceries.models import Base

engine = create_engine('sqlite:///db.sqlite', echo=True)
Base.metadata.create_all(engine)
