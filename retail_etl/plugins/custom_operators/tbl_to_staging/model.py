from datetime import date as date

from dataclasses import dataclass
from typing import Optional, Text


@dataclass(frozen=True)
class Column:
    name: str
    data_type: object


@dataclass(frozen=True)
class Header:
    def to_list(self):
        return list(self.__annotations__)


@dataclass(init=False, frozen=True)
class RegionHeader(Header):
    r_regionkey: Column = Column(name="r_region_key", data_type=int)
    r_name: Column = Column(name="r_name", data_type=Text)
    r_comment: Column = Column(name="r_comment", data_type=Optional[Text])


@dataclass(init=False, frozen=True)
class NationHeader(Header):
    n_nationkey: Column = Column(name="n_nationkey", data_type=int)
    n_name: Column = Column(name="n_name", data_type=Text)
    n_regionkey: Column = Column(name="n_regionkey", data_type=int)
    n_comment: Column = Column(name="n_comment", data_type=Optional[Text])


@dataclass(init=False, frozen=True)
class PartHeader(Header):
    p_partkey: Column = Column(name="p_partkey", data_type=int)
    p_name: Column = Column(name="p_name", data_type=Text)
    p_mfgr: Column = Column(name="p_mfgr", data_type=Text)
    p_brand: Column = Column(name="p_brand", data_type=Text)
    p_type: Column = Column(name="p_type", data_type=Text)
    p_size: Column = Column(name="p_size", data_type=int)
    p_container: Column = Column(name="p_container", data_type=Text)
    p_retailprice: Column = Column(name="p_retailprice", data_type=int)
    p_comment: Column = Column(name="p_comment", data_type=Text)


@dataclass(init=False, frozen=True)
class SupplierHeader(Header):
    s_suppkey: Column = Column(name="p_partkey", data_type=int)
    s_name: Column = Column(name="p_name", data_type=Text)
    s_address: Column = Column(name="p_mfgr", data_type=Text)
    s_nationkey: Column = Column(name="p_brand", data_type=int)
    s_phone: Column = Column(name="p_type", data_type=Text)
    s_acctbal: Column = Column(name="p_size", data_type=int)
    s_comment: Column = Column(name="p_container", data_type=Text)


@dataclass(init=False, frozen=True)
class PartSuppHeader(Header):
    ps_partkey: Column = Column(name="p_partkey", data_type=int)
    ps_suppkey: Column = Column(name="p_name", data_type=int)
    ps_availqty: Column = Column(name="p_mfgr", data_type=int)
    ps_supplycost: Column = Column(name="p_brand", data_type=int)
    ps_comment: Column = Column(name="p_type", data_type=Text)


@dataclass(init=False, frozen=True)
class CustomerHeader(Header):
    c_custkey: Column = Column(name="c_custkey", data_type=int)
    c_name: Column = Column(name="c_name", data_type=Text)
    c_address: Column = Column(name="c_address", data_type=Text)
    c_nationkey: Column = Column(name="c_nationkey", data_type=int)
    c_phone: Column = Column(name="c_phone", data_type=Text)
    c_acctbal: Column = Column(name="c_acctbal", data_type=int)
    c_mktsegment: Column = Column(name="c_mktsegment", data_type=Text)
    c_comment: Column = Column(name="c_comment", data_type=Text)


@dataclass(init=False, frozen=True)
class OrderHeader(Header):
    o_ordkey: Column = Column(name="o_ordkey", data_type=int)
    o_custkey: Column = Column(name="o_custkey", data_type=int)
    o_orderstatus: Column = Column(name="o_orderstatus", data_type=Text)
    o_totalprice: Column = Column(name="o_totalprice", data_type=int)
    o_orderdate: Column = Column(name="o_orderdate", data_type=date)
    o_orderpriority: Column = Column(name="o_orderpriority", data_type=Text)
    o_clerk: Column = Column(name="o_clerk", data_type=Text)
    o_shippriority: Column = Column(name="o_shippriority", data_type=int)
    o_comment: Column = Column(name="o_comment", data_type=Text)


@dataclass(init=False, frozen=True)
class LineItemHeader(Header):
    l_orderkey: Column = Column(name="l_orderkey", data_type=int)
    l_partkey: Column = Column(name="l_partkey", data_type=int)
    l_suppkey: Column = Column(name="l_suppkey", data_type=Text)
    l_linenumber: Column = Column(name="l_linenumber", data_type=int)
    l_quantity: Column = Column(name="l_quantity", data_type=int)
    l_extendedprice: Column = Column(name="l_extendedprice", data_type=int)
    l_discount: Column = Column(name="l_discount", data_type=int)
    l_tax: Column = Column(name="l_tax", data_type=int)
    l_returnflag: Column = Column(name="l_returnflag", data_type=Text)
    l_linestatus: Column = Column(name="l_linestatus", data_type=Text)
    l_shipdate: Column = Column(name="l_shipdate", data_type=date)
    l_commitdate: Column = Column(name="l_commitdate", data_type=date)
    l_receiptdate: Column = Column(name="l_receiptdate", data_type=date)
    l_shipinstruct: Column = Column(name="l_shipinstruct", data_type=Text)
    l_shipmode: Column = Column(name="l_shipmode", data_type=Text)
    l_comment: Column = Column(name="l_comment", data_type=Text)
