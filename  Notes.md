## Tushare Pro API Limitations:

* **积分与频次权限对应表**: https://tushare.pro/document/1?doc_id=290
* **Tushare Pro的变化**: https://tushare.pro/document/1
* **标准Tushare API**: http://tushare.org/Tushare/index.html


## Save Stock Data

### Influx Storage Primitives:
- **Point**: is a single data record consists of measurement, tag sets, field sets and a timestamp. *It is uniquely identified by the measurement name, tag set, and timestamp.*
- **Line Protocol**: is the text representation of a data point that's readable by Influx to craete the record. Syntax is `<measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]`.

### Write to Influx

The suggested option is to upload an [extended annotation CSV](https://docs.influxdata.com/influxdb/v2.3/reference/syntax/annotated-csv/extended/). Or use the [influx write](https://docs.influxdata.com/influxdb/v2.3/reference/cli/influx/write/) and give some line protocol inputs.

### Design the Schema
* **Tag**: These values are indexed and field values aren’t. This means that querying tags is more performant than querying fields. Must avoid using [flux keyword](https://docs.influxdata.com/flux/v0.x/spec/lexical-elements/#keywords) as the tag key
* **Field**: These are the high varaible data you want to gather, and should be numeric values.
* **Cardinality**: The number of unique measurement, tag set, and field key combinations in an InfluxDB bucket. See [detailed description](https://docs.influxdata.com/influxdb/v2.3/reference/glossary/#series-cardinality)


## Query the Stock Data
* Check out the [Flux Query Language](https://docs.influxdata.com/flux/v0.x/get-started/)
* Important concept:
    - a query creates streams of tables, where each table holds a collection of results.
    - a stream of table is uniquely identified by its group key, which contains the query condition and retrieved fields
