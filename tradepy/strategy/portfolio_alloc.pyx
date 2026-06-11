# cython: language_level=3
# pyright: reportArgumentType=false, reportAssignmentType=false, reportGeneralTypeIssues=false, reportIndexIssue=false

from libc.math cimport ceil, floor
from libc.stdlib cimport free, malloc
from random import randrange, sample
from collections import namedtuple

BudgetAllocation = namedtuple("BudgetAllocation", ["code", "price", "vol"])


cdef Py_ssize_t TRADE_LOT_VOL = 100


cdef struct Candidate:
    Py_ssize_t idx  # position in the input arrays
    Py_ssize_t lots
    Py_ssize_t max_lots
    double lot_cost


def portfolio_alloc(
    object codes,
    const double[:] buy_prices,
    double budget,
    int max_opens_count,
    double position_max_value,
    double position_min_value,
):
    """Pick at most ``max_opens_count`` names and size each one in whole trade
    lots, keeping every position's notional within ``[position_min_value,
    position_max_value]`` and the total within ``budget``, spreading the budget
    as evenly across names as the lot sizes permit.
    """
    assert buy_prices.shape[0] == len(codes)

    cdef Py_ssize_t n_codes = len(codes)
    if n_codes == 0 or max_opens_count <= 0:
        return []

    cdef list picked = (
        sample(range(n_codes), max_opens_count)
        if n_codes > max_opens_count
        else list(range(n_codes))
    )

    cdef Candidate* cands = <Candidate*>malloc(len(picked) * sizeof(Candidate))
    if cands == NULL:
        raise MemoryError()

    cdef Py_ssize_t count = 0, idx, min_lots, max_lots, best, vol, i
    cdef double lot_cost, value, best_value, spent = 0
    cdef double price, remaining
    cdef Candidate* c
    cdef object code
    cdef list allocations = []

    try:
        # Keep names whose minimum buy satisfies the position bounds and budget
        for idx in picked:
            lot_cost = buy_prices[idx] * TRADE_LOT_VOL
            min_lots = max(<Py_ssize_t>ceil(position_min_value / lot_cost), 1)
            max_lots = <Py_ssize_t>floor(position_max_value / lot_cost)
            if min_lots > max_lots or min_lots * lot_cost > budget:
                continue

            cands[count] = Candidate(
                idx=idx, lots=min_lots, max_lots=max_lots, lot_cost=lot_cost
            )
            spent += min_lots * lot_cost
            count += 1

        # Randomly drop names until all the minimum buys fit within the budget
        while count > 0 and spent > budget:
            i = randrange(count)
            spent -= cands[i].lots * cands[i].lot_cost
            count -= 1
            cands[i] = cands[count]

        # Spend the leftover one lot at a time on the currently smallest
        # position, which converges to an even split across names
        remaining = budget - spent
        while True:
            best = -1
            for i in range(count):
                c = &cands[i]
                if c.lots >= c.max_lots or c.lot_cost > remaining:
                    continue

                value = c.lots * c.lot_cost
                if best < 0 or value < best_value:
                    best = i
                    best_value = value

            if best < 0:
                break

            cands[best].lots += 1
            remaining -= cands[best].lot_cost

        for i in range(count):
            c = &cands[i]
            code = codes[c.idx]
            price = buy_prices[c.idx]
            vol = c.lots * TRADE_LOT_VOL
            allocations.append(BudgetAllocation(
                code, price, vol
            ))

        return allocations
    finally:
        free(cands)
