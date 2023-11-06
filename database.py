import os
import uuid
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker, relationship, Mapped
from sqlalchemy import Enum as SQLAlchemyEnum
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from enum import Enum
from typing import List

load_dotenv(find_dotenv())

DATABASE_URL = os.getenv("DATABASE_URL")
Base = declarative_base()

class TaskStatusEnum(Enum):
    SCHEDULED = "scheduled"
    SENT = "sent"
    OPENED = "opened"
    SENDING_FAILED = "sending_failed"

class EmailTask(Base):
    __tablename__ = "email_tasks"

    id = Column(UUID(as_uuid=True), default=uuid.uuid4, primary_key=True, index=True)
    batch_id = Column(UUID, ForeignKey("batches.id"), nullable=True)
    recipient_email = Column(String, index=True, unique=True)
    email_content = Column(String)
    status = Column(SQLAlchemyEnum(TaskStatusEnum, native_enum=False), default=TaskStatusEnum.SCHEDULED, index=True, nullable=False)
    batch: Mapped["Batch"] = relationship(back_populates="email_tasks")
    created_at = Column(DateTime, default=datetime.utcnow())
    updated_at = Column(DateTime, default=datetime.utcnow())

class Batch(Base):
    __tablename__ = "batches"

    id = Column(UUID(as_uuid=True), default=uuid.uuid4, primary_key=True, index=True)
    country = Column(String, index=True)
    scheduled_processing_time = Column(DateTime)
    email_tasks: Mapped[List["EmailTask"]] = relationship(back_populates="batch")
    created_at = Column(DateTime, default=datetime.utcnow())

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)