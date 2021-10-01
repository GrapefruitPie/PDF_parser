import camelot
import db
from tqdm import tqdm


def parse_head(df, page):
    vals = dict()
    s = df.iloc[0, 1]
    if s.strip() == '':
        s = df.iloc[0, 0].split('FROM:')[1].strip()
    vals['page'] = page
    vals['from'] = s
    vals['from_airport'] = df.iloc[0, 6]
    s = df.iloc[1, 1]
    if s.strip() == '':
        s = df.iloc[1, 0].split('TO:')[1].strip()
    vals['to'] = s
    vals['to_airport'] = df.iloc[1, 6]
    pass
    parse_tail_entries(df, vals, range(4, df.shape[0]))
    return vals


def parse_tail(df, vals):
    if vals is None:
        raise Exception("Can't parse tail without vals")
    parse_tail_entries(df, vals, range(df.shape[0]))
    return vals


def parse_tail_entries(df, vals, r):
    for i in r:
        row = df.iloc[i, :]
        if 'Operated by:' in row.to_string():
            continue
        entry = vals.copy()
        entry.update(parse_tail_entry(row))
        for key in entry.keys():
            if entry[key] == '':
                pass
        db.timetable.insert_one(entry)


def parse_tail_entry(raw):
    vals = dict()
    vals['validity'] = raw[0]
    vals['days'] = str(raw[1])
    vals['departure_time'] = raw[2]
    vals['arrival_time'] = raw[3]
    vals['flight'] = raw[4]
    vals['aircraft'] = raw[5]
    vals['travel_time'] = raw[6]
    return vals


def parse_table(page, area, vals=None):
    tables = None
    try:
        tables = camelot.read_pdf(f'./files/Skyteam_Timetable/{page}.pdf', flavor='stream',
                                       table_areas=[area])
    except:
        return
    if tables is None:
        return
    df = tables[0].df
    if 'Consult your travel agent' in df.to_string():
        return
    if df.shape[1] != 7:
        with open('err.txt', 'a') as f:
            f.write(str(page) + '\n')
        print(f'FAIL: {page}')
        return
    if 'FROM' in df.iloc[0, 0]:
        vals = parse_head(df, page)
    else:
        parse_tail(df, vals)
    return vals


left_area = '14,807,289,54'
right_area = '304,807,580,53'

left_vals = None
right_vals = None

for page in tqdm(range(5, 27514)):
    left_vals = parse_table(page, left_area, left_vals)
    right_vals = parse_table(page, right_area, right_vals)

