#!/usr/bin/env python3
"""Strip Rust comments preserving string/char literals."""
import os
import sys

def strip_rust(src: str) -> str:
    out = []
    i = 0
    n = len(src)
    while i < n:
        c = src[i]
        nxt = src[i+1] if i+1 < n else ''
        # Raw string r#"..."#  or r"..."
        if c == 'r' and (nxt == '"' or nxt == '#'):
            j = i + 1
            hashes = 0
            while j < n and src[j] == '#':
                hashes += 1
                j += 1
            if j < n and src[j] == '"':
                j += 1
                close = '"' + ('#' * hashes)
                end = src.find(close, j)
                if end == -1:
                    out.append(src[i:])
                    return ''.join(out)
                out.append(src[i:end+len(close)])
                i = end + len(close)
                continue
        # Byte string b"..." or byte char b'...'
        if c == 'b' and (nxt == '"' or nxt == "'"):
            out.append(c)
            i += 1
            continue
        # Regular string "..."
        if c == '"':
            j = i + 1
            while j < n:
                if src[j] == '\\' and j+1 < n:
                    j += 2
                    continue
                if src[j] == '"':
                    j += 1
                    break
                j += 1
            out.append(src[i:j])
            i = j
            continue
        # Char literal '...'
        if c == "'":
            # Could be a lifetime: 'a, 'static, '_ - not a char literal
            # Char literal patterns: 'x' or '\n' or '\\' or '\x41' or '\u{1F600}'
            j = i + 1
            if j < n:
                if src[j] == '\\':
                    # escape: scan to closing '
                    k = j + 1
                    # \u{...}
                    if k < n and src[k] == 'u' and k+1 < n and src[k+1] == '{':
                        end_brace = src.find('}', k+2)
                        if end_brace != -1 and end_brace+1 < n and src[end_brace+1] == "'":
                            out.append(src[i:end_brace+2])
                            i = end_brace + 2
                            continue
                    # \xNN or single-char escape
                    if k < n:
                        if src[k] == 'x' and k+3 < n and src[k+3] == "'":
                            out.append(src[i:k+4])
                            i = k+4
                            continue
                        if k+1 < n and src[k+1] == "'":
                            out.append(src[i:k+2])
                            i = k+2
                            continue
                    # fall through - treat as not a char literal
                else:
                    # 'x' char or 'lifetime
                    if j+1 < n and src[j+1] == "'":
                        out.append(src[i:j+2])
                        i = j+2
                        continue
            # lifetime: emit '
            out.append(c)
            i += 1
            continue
        # Line comment
        if c == '/' and nxt == '/':
            # consume to end of line
            j = i
            while j < n and src[j] != '\n':
                j += 1
            # leave the newline in place
            i = j
            continue
        # Block comment (nested)
        if c == '/' and nxt == '*':
            depth = 1
            j = i + 2
            while j < n and depth > 0:
                if src[j] == '/' and j+1 < n and src[j+1] == '*':
                    depth += 1
                    j += 2
                elif src[j] == '*' and j+1 < n and src[j+1] == '/':
                    depth -= 1
                    j += 2
                else:
                    j += 1
            i = j
            continue
        out.append(c)
        i += 1
    return ''.join(out)

def cleanup_blank_lines(src: str) -> str:
    lines = src.split('\n')
    cleaned = []
    blank_run = 0
    for line in lines:
        if line.strip() == '':
            blank_run += 1
            if blank_run <= 1:
                cleaned.append('')
        else:
            blank_run = 0
            cleaned.append(line.rstrip())
    # Strip leading blank lines
    while cleaned and cleaned[0] == '':
        cleaned.pop(0)
    # Ensure single trailing newline
    while len(cleaned) > 1 and cleaned[-1] == '':
        cleaned.pop()
    return '\n'.join(cleaned) + '\n'

def process_file(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        src = f.read()
    stripped = strip_rust(src)
    cleaned = cleanup_blank_lines(stripped)
    if cleaned != src:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(cleaned)
        return True
    return False

def main():
    roots = ['bytedb-core', 'bytedb-query', 'bytedb-server', 'bytedb-client', 'bytedb-bench', 'tests']
    changed = 0
    total = 0
    for root in roots:
        if not os.path.isdir(root):
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            if 'target' in dirpath.split(os.sep):
                continue
            for fn in filenames:
                if fn.endswith('.rs'):
                    path = os.path.join(dirpath, fn)
                    total += 1
                    if process_file(path):
                        changed += 1
                        print(f'  stripped: {path}')
    print(f'\nProcessed {total} files, modified {changed}')

if __name__ == '__main__':
    main()
