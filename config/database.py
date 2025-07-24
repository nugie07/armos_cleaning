import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# Database A (Main Database) Configuration
DB_A_HOST = os.getenv("DB_A_HOST", "localhost")
DB_A_PORT = os.getenv("DB_A_PORT", "5432")
DB_A_NAME = os.getenv("DB_A_NAME", "main_database")
DB_A_USER = os.getenv("DB_A_USER", "postgres")
DB_A_PASSWORD = os.getenv("DB_A_PASSWORD", "")

# Database B (Warehouse Cleaning Database) Configuration
DB_B_HOST = os.getenv("DB_B_HOST", "localhost")
DB_B_PORT = os.getenv("DB_B_PORT", "5432")
DB_B_NAME = os.getenv("DB_B_NAME", "warehouse_cleaning")
DB_B_USER = os.getenv("DB_B_USER", "postgres")
DB_B_PASSWORD = os.getenv("DB_B_PASSWORD", "")

# Database A URL
DATABASE_A_URL = f"postgresql://{DB_A_USER}:{DB_A_PASSWORD}@{DB_A_HOST}:{DB_A_PORT}/{DB_A_NAME}"

# Database B URL
DATABASE_B_URL = f"postgresql://{DB_B_USER}:{DB_B_PASSWORD}@{DB_B_HOST}:{DB_B_PORT}/{DB_B_NAME}"

# Create engines
engine_a = create_engine(DATABASE_A_URL)
engine_b = create_engine(DATABASE_B_URL)

# Create session factories
SessionLocalA = sessionmaker(autocommit=False, autoflush=False, bind=engine_a)
SessionLocalB = sessionmaker(autocommit=False, autoflush=False, bind=engine_b)

# Base class for models
Base = declarative_base()

def get_db_a():
    """Get Database A session"""
    db = SessionLocalA()
    try:
        yield db
    finally:
        db.close()

def get_db_b():
    """Get Database B session"""
    db = SessionLocalB()
    try:
        yield db
    finally:
        db.close() 