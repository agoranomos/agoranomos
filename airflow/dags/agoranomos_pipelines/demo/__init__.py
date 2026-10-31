"""The demo generator of agoranomos: a fictional business on four fictional platforms and a
source of exchange rates, in the shapes of the APIs that the platforms mimic (ADR-0025).

The generator fakes the transport, not the data: each platform yields the records that the API it
mimics would send, and dlt loads them as it is to load those of a real connector. common.py holds
the rules that every platform shares, world.py the business, one module each platform, shapes.py
the fields of every record, platforms.py the list of the platforms, and sources.py, the only one
that imports dlt, their dlt sources.
"""
