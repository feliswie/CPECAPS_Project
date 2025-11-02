import pandas as pd
from datetime import datetime, timedelta
import random

def gen_sample(n=20, filename='sample_telemetry.xlsx'):
    rows = []
    now = datetime.utcnow().date()
    areas = ['North', 'South', 'East', 'West', 'HQ']
    for i in range(1, n+1):
        dev = f"{1000+i}"
        # random last sighted within 0..90 days
        days_ago = random.choice(list(range(0,91)))
        last_seen = now - timedelta(days=days_ago)
        area = random.choice(areas)
        usage = random.choice([0, 10, 20, 40, 60, 80, 100])
        status = random.choice(['in circulation','in journey','decommissioned','in transit'])
        rows.append({'Device ID': dev, 'Last Sighted Date': last_seen.strftime('%Y-%m-%d'), 'Location': area+' Depot', 'Area': area, 'Usage': usage, 'Status': status})

    df = pd.DataFrame(rows)
    df.to_excel(filename, index=False)
    print("Saved sample to", filename)

if __name__ == '__main__':
    gen_sample(50, 'sample_telemetry.xlsx')
