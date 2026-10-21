# PostgreSQL ledger operations

Use UNIQUE (payment_id, kind) on ledger_entries and a member_month_locks table keyed by (member_id, month).
Both operations run in a READ COMMITTED transaction; acquire pg_advisory_xact_lock(hashtextextended(payment_id, 0)) first.
Hash collisions only serialize unrelated payments; payment identity is still checked using the original payment_id.
Award: if an award already exists for payment_id, commit without inserting.
Otherwise INSERT the member/month lock row ON CONFLICT DO NOTHING, then SELECT it FOR UPDATE.
After acquiring that lock, use a separate statement to SUM only award entries for the member and UTC settlement month.
Insert min(requested_points, greatest(monthly_cap - awarded_points, 0)), including zero-point awards, then commit.
All award writers must acquire these locks in the same order: payment first, member/month second.
Reverse: under the payment lock, return success if a reversal exists; otherwise read the original award.
If no award exists, commit without writing and log/ack the unknown payment for finance reconciliation.
Otherwise insert a reversal using the original member, settlement month and negated awarded points, then commit.
Reversals never reduce the monthly award sum, so they do not restore cap room.
Payment locks serialize duplicate deliveries and award/reversal races across replicas; uniqueness is an additional safeguard.
Member/month locks serialize cap calculations across distinct payments while other members proceed independently.
Ack only after commit; on storage errors, roll back and Nack. Redelivery after a committed write is a no-op.
MemoryStore models these atomic operations with its storage mutex; simulated I/O and faults run before acquiring it.
WithFaults receives "award" or "reverse" for these operations; a hook error leaves the ledger unchanged.
