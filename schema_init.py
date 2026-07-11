# schema_init.py
import asyncio
from main import Base, engine, Account
import uuid
from decimal import Decimal

async def init_db():
    async with engine.begin() as conn:
        # Drop and create tables cleanly
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    print("✅ PostgreSQL tables initialized successfully!")

    # Optional: Seed one mock account with $1000 so you can test immediately
    from main import async_session
    async with async_session() as session:
        async with session.begin():
            mock_account = Account(
                account_id=uuid.UUID("99999999-9999-9999-9999-999999999999"),
                user_id=uuid.uuid4(),
                balance=Decimal("1000.0000")
            )
            session.add(mock_account)
    print("✅ Seeded test account: 99999999-9999-9999-9999-999999999999 with $1000.0000")

if __name__ == "__main__":
    asyncio.run(init_db())
