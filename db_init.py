# db_init.py
from sqlalchemy import create_engine, Column, String, Text, Boolean, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime

engine = create_engine("sqlite:///leads.db")
Base = declarative_base()

class Prospect(Base):
    __tablename__ = "prospects_raw"
    url = Column(String, primary_key=True)
    company_name = Column(String)
    source_city = Column(String)
    scraped_at = Column(DateTime, default=datetime.utcnow)

class Page(Base):
    __tablename__ = "crawled_pages"
    url = Column(String, primary_key=True)
    text = Column(Text)
    fetched_at = Column(DateTime, default=datetime.utcnow)

class Contact(Base):
    __tablename__ = "contacts"
    url = Column(String, primary_key=True)
    email = Column(String)
    phone = Column(String)
    reason = Column(Text)
    qualified = Column(Boolean)
    drafted_email = Column(Text)
    processed_at = Column(DateTime, default=datetime.utcnow)

Base.metadata.create_all(engine)
print("✅ leads.db initialized")