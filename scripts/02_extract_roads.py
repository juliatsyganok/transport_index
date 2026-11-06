#!/usr/bin/env python3
"""Extract road ways and referenced nodes from OSM PBF without osmium/pyrosm.

Dependencies: pyarrow. Uses only Python stdlib for PBF parsing.
"""
import json
import os
import sqlite3
import struct
import sys
import zlib
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / 'data' / 'raw' / 'osm'
OUT = ROOT / 'data' / 'processed' / 'roads'
REGIONS = ('south-fed-district', 'north-caucasus-fed-district')
# Automotive road candidates. Further access/direction filtering belongs to graph stage.
ROAD_TYPES = {'motorway','motorway_link','trunk','trunk_link','primary','primary_link',
              'secondary','secondary_link','tertiary','tertiary_link','unclassified',
              'residential','living_street','service','road','track'}
BATCH = 10000
MAX_BLOB = 64 * 1024 * 1024


def varint(data, pos):
    val = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ValueError('Truncated protobuf varint')
        b = data[pos]
        pos += 1
        val |= (b & 127) << shift
        if b < 128:
            return val, pos
        shift += 7
        if shift > 70:
            raise ValueError('Invalid protobuf varint')


def fields(data):
    pos = 0
    while pos < len(data):
        key, pos = varint(data, pos)
        num, wire = key >> 3, key & 7
        if wire == 0:
            value, pos = varint(data, pos)
        elif wire == 1:
            value = data[pos:pos+8]
            pos += 8
        elif wire == 2:
            length, pos = varint(data, pos)
            value = data[pos:pos+length]
            pos += length
            if len(value) != length:
                raise ValueError('Truncated protobuf bytes')
        elif wire == 5:
            value = data[pos:pos+4]
            pos += 4
        else:
            raise ValueError(f'Unsupported protobuf wire type {wire}')
        yield num, value


def signed(value, bits=64):
    return value - (1 << bits) if value >= (1 << (bits-1)) else value


def zigzag(value):
    return (value >> 1) ^ -(value & 1)


def packed(buf, transform=lambda x: x):
    pos = 0
    while pos < len(buf):
        v, pos = varint(buf, pos)
        yield transform(v)


def delta(values):
    acc = 0
    for v in values:
        acc += v
        yield acc


def read_blocks(path):
    with path.open('rb') as fh:
        while True:
            header_size = fh.read(4)
            if not header_size:
                break
            if len(header_size) != 4:
                raise ValueError('Truncated PBF header size')
            size = struct.unpack('>I', header_size)[0]
            if size > 65536:
                raise ValueError('Oversized PBF blob header')
            header = fh.read(size)
            if len(header) != size:
                raise ValueError('Truncated PBF blob header')
            kind = None
            blob_size = None
            for num, val in fields(header):
                if num == 1:
                    kind = val.decode('ascii')
                elif num == 3:
                    blob_size = val
            if blob_size is None or blob_size > MAX_BLOB:
                raise ValueError('Invalid PBF blob size')
            blob = fh.read(blob_size)
            if len(blob) != blob_size:
                raise ValueError('Truncated PBF blob')
            if kind != 'OSMData':
                continue
            raw = None
            raw_size = None
            for num, val in fields(blob):
                if num == 1:
                    raw = val
                elif num == 2:
                    raw_size = val
                elif num == 3:
                    raw = zlib.decompress(val)
                elif num in (4, 5, 6, 7):
                    raise ValueError('Unsupported PBF compression (requires raw/zlib)')
            if raw is None:
                raise ValueError('PBF blob has no supported payload')
            if raw_size is not None and len(raw) != raw_size:
                raise ValueError('PBF blob uncompressed size mismatch')
            yield raw


def primitive(raw):
    strings = []
    groups = []
    granularity = 100
    lat_offset = 0
    lon_offset = 0
    for num, val in fields(raw):
        if num == 1:
            strings = [x for tag, x in fields(val) if tag == 1]
        elif num == 2:
            groups.append(val)
        elif num == 17:
            granularity = val
        elif num == 19:
            lat_offset = signed(val)
        elif num == 20:
            lon_offset = signed(val)
    return strings, groups, granularity, lat_offset, lon_offset


def way_record(raw, strings):
    ident = None
    keys = []
    vals = []
    refs = []
    for num, val in fields(raw):
        if num == 1:
            ident = signed(val)
        elif num == 2:
            keys.extend(packed(val))
        elif num == 3:
            vals.extend(packed(val))
        elif num == 8:
            refs.extend(packed(val, zigzag))
    tags = {strings[k].decode('utf-8', 'replace'): strings[v].decode('utf-8', 'replace')
            for k, v in zip(keys, vals)}
    return ident, tags, list(delta(refs))


def nodes_from_group(group, gran, lat_offset, lon_offset):
    for num, val in fields(group):
        if num == 1:  # Non-dense Node
            ident = lat = lon = None
            for tag, item in fields(val):
                if tag == 1:
                    ident = zigzag(item)
                elif tag == 8:
                    lat = zigzag(item)
                elif tag == 9:
                    lon = zigzag(item)
            if None not in (ident, lat, lon):
                yield ident, (lon_offset + gran * lon) * 1e-9, (lat_offset + gran * lat) * 1e-9
        elif num == 2:  # DenseNodes
            ids = lats = lons = []
            for tag, item in fields(val):
                if tag == 1:
                    ids = list(delta(packed(item, zigzag)))
                elif tag == 8:
                    lats = list(delta(packed(item, zigzag)))
                elif tag == 9:
                    lons = list(delta(packed(item, zigzag)))
            if not (len(ids) == len(lats) == len(lons)):
                raise ValueError('Invalid dense nodes')
            for ident, lat, lon in zip(ids, lats, lons):
                yield ident, (lon_offset + gran * lon) * 1e-9, (lat_offset + gran * lat) * 1e-9


def write_batch(writer, records, schema, pa):
    if records:
        writer.write_table(pa.Table.from_pylist(records, schema=schema))
        records.clear()


def process(region, pa, pq):
    source = RAW / f'{region}.osm.pbf'
    if not source.is_file():
        raise FileNotFoundError(source)
    dest = OUT / region
    dest.mkdir(parents=True, exist_ok=True)
    db_path = dest / 'needed_nodes.sqlite'
    ways_tmp = dest / 'ways.parquet.part'
    nodes_tmp = dest / 'nodes.parquet.part'
    for p in (db_path, ways_tmp, nodes_tmp):
        p.unlink(missing_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode=OFF')
    conn.execute('PRAGMA synchronous=OFF')
    conn.execute('CREATE TABLE needed (id INTEGER PRIMARY KEY)')
    way_schema = pa.schema([
        ('osm_way_id', pa.int64()), ('node_ids', pa.list_(pa.int64())),
        ('highway', pa.string()), ('name', pa.string()), ('ref', pa.string()),
        ('oneway', pa.string()), ('maxspeed', pa.string()), ('surface', pa.string()),
        ('access', pa.string()), ('motor_vehicle', pa.string()),
        ('bridge', pa.string()), ('tunnel', pa.string()), ('junction', pa.string()),
        ('region', pa.string())])
    node_schema = pa.schema([
        ('node_id', pa.int64()), ('lon', pa.float64()), ('lat', pa.float64()),
        ('region', pa.string())])
    way_count = node_count = 0
    print(f'\n{region}: проход 1/2 — автомобильные дороги', flush=True)
    try:
        with pq.ParquetWriter(ways_tmp, way_schema, compression='zstd') as writer:
            rows = []
            ids_to_insert = []
            for raw in read_blocks(source):
                strings, groups, _, _, _ = primitive(raw)
                for group in groups:
                    for tag, item in fields(group):
                        if tag != 3:  # Way
                            continue
                        ident, tags, refs = way_record(item, strings)
                        if tags.get('highway') not in ROAD_TYPES or len(refs) < 2:
                            continue
                        rows.append({'osm_way_id': ident, 'node_ids': refs,
                                     **{k: tags.get(k) for k in ('highway','name','ref','oneway',
                                         'maxspeed','surface','access','motor_vehicle','bridge','tunnel','junction')},
                                     'region': region})
                        ids_to_insert.extend((x,) for x in refs)
                        way_count += 1
                        if len(rows) >= BATCH:
                            write_batch(writer, rows, way_schema, pa)
                        if len(ids_to_insert) >= 100000:
                            conn.executemany('INSERT OR IGNORE INTO needed(id) VALUES (?)', ids_to_insert)
                            conn.commit()
                            ids_to_insert.clear()
            write_batch(writer, rows, way_schema, pa)
            if ids_to_insert:
                conn.executemany('INSERT OR IGNORE INTO needed(id) VALUES (?)', ids_to_insert)
                conn.commit()
        expected = conn.execute('SELECT COUNT(*) FROM needed').fetchone()[0]
        print(f'  Дорог: {way_count:,}; требуемых уникальных узлов: {expected:,}', flush=True)
        print(f'{region}: проход 2/2 — координаты узлов', flush=True)
        with pq.ParquetWriter(nodes_tmp, node_schema, compression='zstd') as writer:
            rows = []
            for raw in read_blocks(source):
                _, groups, gran, lat_off, lon_off = primitive(raw)
                for group in groups:
                    for ident, lon, lat in nodes_from_group(group, gran, lat_off, lon_off):
                        if conn.execute('SELECT 1 FROM needed WHERE id=?', (ident,)).fetchone():
                            rows.append({'node_id': ident, 'lon': lon, 'lat': lat, 'region': region})
                            node_count += 1
                            if len(rows) >= BATCH:
                                write_batch(writer, rows, node_schema, pa)
            write_batch(writer, rows, node_schema, pa)
        # File-level atomic replacement. Prior good files remain if processing fails.
        os.replace(ways_tmp, dest / 'ways.parquet')
        os.replace(nodes_tmp, dest / 'nodes.parquet')
        manifest = {'region': region, 'source': str(source.relative_to(ROOT)),
                    'extractor': 'pure-python-osm-pbf',
                    'created_utc': datetime.now(timezone.utc).isoformat(),
                    'ways': way_count, 'unique_node_ids_needed': expected,
                    'node_records_found': node_count,
                    'possible_missing_nodes': max(0, expected - node_count),
                    'outputs': ['ways.parquet', 'nodes.parquet'],
                    'note': 'Candidate road ways; access/direction/topology filtering is next stage.'}
        temp = dest / 'extract_manifest.json.tmp'
        temp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(temp, dest / 'extract_manifest.json')
        print(f'  Готово: {way_count:,} дорог; {node_count:,} записей узлов', flush=True)
        if expected != node_count:
            print('  ВНИМАНИЕ: число записей узлов отличается от числа уникальных ID; требуется проверка.', flush=True)
    finally:
        conn.close()
        db_path.unlink(missing_ok=True)


def main():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except (ImportError, OSError) as exc:
        print(f'Не удалось импортировать pyarrow: {exc}', file=sys.stderr)
        return 1
    OUT.mkdir(parents=True, exist_ok=True)
    print('Этап 2: извлечение дорог из OSM PBF (без osmium и pyrosm)', flush=True)
    for region in REGIONS:
        try:
            process(region, pa, pq)
        except Exception as exc:
            print(f'Ошибка обработки {region}: {exc}', file=sys.stderr)
            raise
    print('\nОба округа обработаны.', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
