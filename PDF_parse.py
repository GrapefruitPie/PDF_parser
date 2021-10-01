import tabula
import db
from tqdm import tqdm


'''
FROM
TO

airport code 1
airport code 2

Validity
Days, Dep Time , Arr Time, Flight
Aircraft
Travel Time
'''


def parse_head(df):
    vals = dict()
    vals['from'] = df.columns.values[0][6:] #substring from header
    vals['from_airport'] = df.columns.values[3]
    vals['to'] = df.iloc[0, :][0][3:] #substring from raw element
    vals['to_airport'] = df.iloc[0, :][3]
    pass
    for i in range(3, df.shape[0]):
        entry = vals.copy()
        entry.update(parse_head_entry(df.iloc[i, :]))
        db.timetable.insert_one(entry)
    return vals


def parse_tail(df, vals):
    entry = vals.copy()
    entry.update(parse_tail_entry(df.columns))
    db.timetable.insert_one(entry)
    for i in range(0, df.shape[0]):
        entry = vals.copy()
        entry.update(parse_tail_entry(df.iloc[i, :]))
        db.timetable.insert_one(entry)
    return vals


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


def parse_head_entry(raw): #строка датафрейма
    vals = dict()
    vals['validity'] = raw[0]
    vals['days'] = raw[1].split(' ')[0]
    vals['departure_time'] = raw[1].split(' ')[1]
    vals['arrival_time'] = raw[1].split(' ')[2]
    vals['flight'] = raw[1].split(' ')[3]
    vals['aircraft'] = raw[2]
    vals['travel_time'] = raw[3]
    return vals


doc = tabula.read_pdf('./files/Skyteam_Timetable.pdf', pages='31', stream=True)
left_vals = None
right_vals = None
for page in tqdm(doc):
    if page.shape[1] not in [8, 14]:
        continue
    if page.shape[1] == 8:
        pass #this needs to be done
    if 'FROM' in page.columns.values[0]:
        offset = 4
    else:
        offset = 7
    left_df = page.iloc[:, :offset]
    right_df = page.iloc[:, offset:]
    if 'FROM' in page.columns.values[0]:
        left_vals = parse_head(left_df)
        right_vals = parse_head(right_df)
    else:
        parse_tail(left_df, left_vals)
        parse_tail(right_df, right_vals)
