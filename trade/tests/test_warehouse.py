from trade.warehouse import TicksDepot


repo = TicksDepot("daily.index")
df = repo.load_index_ticks()

print(df.dtypes)
print(df.groupby("code").count())
