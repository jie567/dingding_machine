import  re,datetime
import os


def normalize_eline(eline_str):
    if not eline_str or len(eline_str) % 3 != 0:
        return None
    airports = [eline_str[i:i + 3] for i in range(0, len(eline_str), 3)]
    return ''.join(sorted(airports))

def normalize_route(route_str):
    route_str = route_str.replace('-', '')
    if len(route_str) == 6:
        segment1 = route_str[:3]
        segment2 = route_str[3:]
        return ''.join(sorted([segment1, segment2]))
    else:
        n = 3
        airports = [route_str[i:i + n] for i in range(0, len(route_str), n)]
        return ''.join(sorted(set(airports)))


def cell_to_indices(cell_address):
    col_letters = re.match(r'[A-Z]+', cell_address).group()
    row_number = int(re.search(r'\d+', cell_address).group())

    # 将列字母转换为数字
    col_number = 0
    for i, c in enumerate(col_letters[::-1]):
        col_number += (ord(c.upper()) - 64) * (26 ** i)

    return row_number, col_number

