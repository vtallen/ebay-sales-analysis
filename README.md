# ebay-sales-analysis

## Pitfalls
* 24exp rolls of film are treated as 36exp rolls for simplicity. I no longer sell that SKU and only 5 or so orders were made for 24exp rolls
* The dictionary that stores the item title matching regex expressions must be ordered by most specific to least specific. My regex-title matching function returns the first match it encounters
