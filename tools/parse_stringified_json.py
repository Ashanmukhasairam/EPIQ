import argparse
import json
from json import JSONDecoder
import sys


def parse_file(path):
    s = open(path, 'r', encoding='utf-8').read()
    dec = JSONDecoder()
    idx = 0
    objs = []
    length = len(s)
    while idx < length:
        # skip whitespace
        while idx < length and s[idx].isspace():
            idx += 1
        if idx >= length:
            break
        try:
            obj, end = dec.raw_decode(s, idx)
        except ValueError:
            # can't decode more JSON objects
            break
        idx = end
        if isinstance(obj, dict) and 'raw_json' in obj and isinstance(obj['raw_json'], str):
            try:
                inner = json.loads(obj['raw_json'])
                objs.append(inner)
            except Exception:
                objs.append(obj['raw_json'])
        else:
            objs.append(obj)
    return objs


def main():
    p = argparse.ArgumentParser(description='Parse stringified JSON values inside a file and write pretty JSON')
    p.add_argument('input', help='Input file path')
    p.add_argument('--output', '-o', help='Output file path (defaults to parsed.json next to input)')
    p.add_argument('--json-lines', action='store_true', help='Write output as JSON Lines (one object per line)')
    args = p.parse_args()

    objs = parse_file(args.input)
    if not args.output:
        out = 'parsed.json'
    else:
        out = args.output

    if args.json_lines:
        with open(out, 'w', encoding='utf-8') as f:
            for o in objs:
                f.write(json.dumps(o, ensure_ascii=False))
                f.write('\n')
    else:
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(objs, f, ensure_ascii=False, indent=2)

    print(f'Wrote {len(objs)} objects to {out}')


if __name__ == '__main__':
    main()
