import asyncio
import json
import logging
import os
import uuid  # ◄ ADD THIS IMPORT
from aiokafka import AIOKafkaProducer
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB 
# Setup production logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("OutboxWorker")

# ==========================================
# 1. DATABASE & KAFKA CONFIGURATION
# ==========================================
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/ledger")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_SERVERS", "localhost:9092")
KAFKA_TOPIC = "financial-ledger-events"
BATCH_SIZE = 100  # Number of events to process at once for high throughput

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class TransactionOutbox(Base):
    __tablename__ = "transaction_outbox"
    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True) # ◄ FIXED: Change 'str' to 'uuid.UUID'
    aggregate_id: Mapped[uuid.UUID] = mapped_column()             # ◄ FIXED: Change 'str' to 'uuid.UUID'
    event_type: Mapped[str] = mapped_column()
    payload: Mapped[dict] = mapped_column(JSONB)
    is_processed: Mapped[bool] = mapped_column(default=False)

# ==========================================
# 2. CORE DAEMON LOGIC
# ==========================================
async def process_outbox(producer: AIOKafkaProducer):
    async with async_session() as session:
        async with session.begin(): # ◄ This starts a PostgreSQL transaction
            stmt = (
                select(TransactionOutbox)
                .where(TransactionOutbox.is_processed == False)
                .order_by(TransactionOutbox.event_id)
                .limit(BATCH_SIZE)
                .with_for_update(skip_locked=True)
            )
            result = await session.execute(stmt)
            events = result.scalars().all()

            if not events:
                return False  

            logger.info(f"Picked up {len(events)} events from outbox table.")

            # Send to Kafka
            tasks = []
            for event in events:
                payload_bytes = json.dumps(event.payload).encode("utf-8")
                partition_key = str(event.aggregate_id).encode("utf-8")
                tasks.append(producer.send_and_wait(KAFKA_TOPIC, payload_bytes, key=partition_key))
            
            await asyncio.gather(*tasks)

            # CHANGED: Explicitly mutate the tracking fields and flush to the DB
            for event in events:
                event.is_processed = True
            
            # FORCE SQL UPDATE TRIGGER
            await session.flush() # ◄ ADD THIS LINE TO WRITE UPDATES BEFORE COMMIT
            await session.commit()

            logger.info(f"Successfully synced and committed {len(events)} events to Kafka.")
            return True


# ==========================================
# 3. INFINITE LONGRUNNING LOOP & CRASH RECOVERY
# ==========================================
async def main():
    logger.info("Initializing Outbox Worker Daemon...")
    
    # Initialize high-performance async Kafka producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    logger.info("Connected securely to Kafka broker.")

    try:
        while True:
            try:
                # Process events. If records were found, run again immediately.
                # If the outbox is completely empty, sleep for 1 second to avoid straining the CPU.
                has_work = await process_outbox(producer)
                if not has_work:
                    await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Error processing outbox database records: {str(e)}")
                await asyncio.sleep(5.0)  # Cool-down wait before attempting reconnection
    finally:
        await producer.stop()
        await engine.dispose()
        logger.info("Daemon shut down cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
