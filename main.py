import os
import uuid
from decimal import Decimal
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field, condecimal
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import JSONB

# ==========================================
# 1. DATABASE CONFIGURATION & MODELS
# ==========================================

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost:5432/ledger")

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class Account(Base):
    __tablename__ = "accounts"
    account_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    balance: Mapped[Decimal] = mapped_column(default=Decimal("0.0000"))

class Transaction(Base):
    __tablename__ = "transactions"
    transaction_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    type: Mapped[str] = mapped_column(nullable=False)  # 'DEBIT' or 'CREDIT'
    amount: Mapped[Decimal] = mapped_column(nullable=False)
    idempotency_key: Mapped[str] = mapped_column(unique=True, nullable=False)

class TransactionOutbox(Base):
    __tablename__ = "transaction_outbox"
    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    aggregate_type: Mapped[str] = mapped_column(default="TRANSACTION")
    aggregate_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    event_type: Mapped[str] = mapped_column(nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False) # ◄ CHANGED: Added JSONB here
    is_processed: Mapped[bool] = mapped_column(default=False)

# Dependency to get database sessions securely per request
async def get_db():
    async with async_session() as session:
        yield session

# ==========================================
# 2. PYDANTIC REQUEST VALIDATION SCHEMAS
# ==========================================

class TransactionRequest(BaseModel):
    account_id: uuid.UUID
    type: str = Field(..., pattern="^(DEBIT|CREDIT)$")
    # condecimal forces accurate decimal scales and prevents string/float injection
    amount: condecimal(gt=Decimal("0.0000"), max_digits=15, decimal_places=4) 
    idempotency_key: str = Field(..., min_length=1)
    description: str | None = None


# ==========================================
# 3. HIGH-CONCURRENCY ENDPOINT WITH ROW-LOCKING
# ==========================================

app = FastAPI(title="High-Throughput Ledger API")

@app.post("/v1/transactions", status_code=status.HTTP_201_CREATED)
async def create_transaction(
    payload: TransactionRequest, 
    db: AsyncSession = Depends(get_db)
):
    async with db.begin(): # Starts an atomic transaction block (BEGIN)
        try:
            # Step 1: Lock the single account row exclusively (FOR UPDATE)
            # This forces concurrent requests targeting THIS account to line up sequentially.
            account_stmt = (
                select(Account)
                .where(Account.account_id == payload.account_id)
                .with_for_update()
            )
            result = await db.execute(account_stmt)
            account = result.scalar_one_or_none()
            
            if not account:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, 
                    detail="Account not found"
                )
            
            # Step 2: Calculate new balance state and execute validation guards
            if payload.type == "CREDIT":
                new_balance = account.balance + payload.amount
            else: # DEBIT
                if account.balance < payload.amount:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST, 
                        detail="Insufficient funds for debit transaction"
                    )
                new_balance = account.balance - payload.amount
            
            # Step 3: Write the transaction ledger record
            tx_id = uuid.uuid4()
            new_tx = Transaction(
                transaction_id=tx_id,
                account_id=payload.account_id,
                type=payload.type,
                amount=payload.amount,
                idempotency_key=payload.idempotency_key
            )
            db.add(new_tx)
            
            # Step 4: Update account balance
            account.balance = new_balance
            
            # Step 5: Append to Transactional Outbox (Solves the Dual-Write Problem)
            # The event data is locked in the same transaction block.
            outbox_event = TransactionOutbox(
                aggregate_id=tx_id,
                event_type=f"TRANSACTION_{payload.type}_COMMITTED",
                payload={
                    "transaction_id": str(tx_id),
                    "account_id": str(payload.account_id),
                    "type": payload.type,
                    "amount": float(payload.amount),
                    "new_balance": float(new_balance),
                    "idempotency_key": payload.idempotency_key
                }
            )
            db.add(outbox_event)
            
            # Flush changes within the transaction block to trigger constraints early
            await db.flush()
            
            return {
                "status": "success", 
                "transaction_id": tx_id, 
                "current_balance": new_balance
            }
            
        except IntegrityError as e:
            # Intercepts unique constraint violations (e.g. duplicate idempotency keys)
            # or database check constraint failures.
            await db.rollback()
            if "idempotency_key" in str(e.orig):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Duplicate request. This idempotency key has already been processed."
                )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Transaction failed due to system integrity constraint."
            )

