import numpy as np
import numba as nb


@nb.njit(cache=True)
def evenly_distribute(stocks, budget: float, min_trade_cost: int, trade_lot_vol: int):
    if stocks.shape[0] == 0:
        return stocks

    PriceCol = 1

    # Calculate the minimum number of shares to buy per stock
    n_stocks = len(stocks)
    per_lot_cost = stocks[:, PriceCol] * trade_lot_vol
    total_lots = budget // n_stocks // per_lot_cost
    total_cost = per_lot_cost * total_lots

    # Gather the remaining budget
    remaining_budget = budget - total_cost.sum()

    # Distribute that budget again
    for idx, value in enumerate(per_lot_cost):
        residual_lots = remaining_budget // value
        if residual_lots > 0:
            total_lots[idx] += residual_lots
            remaining_budget -= residual_lots * value

    _min_trade_cost = (per_lot_cost * total_lots).min()
    if _min_trade_cost < min_trade_cost:
        # Randomly drop an option from the portfolio to increase the average trading amount per stock
        remove_idx = np.random.randint(0, n_stocks)
        return evenly_distribute(
            np.concatenate((stocks[:remove_idx], stocks[remove_idx + 1 :])),
            budget,
            min_trade_cost,
            trade_lot_vol,
        )

    buy_lots = stocks.copy()
    buy_lots[:, PriceCol] = total_lots
    return buy_lots
