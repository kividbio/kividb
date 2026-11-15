import sys,re
def load(p):
    d={}
    for l in open(p,encoding='utf8'):
        m=re.match(r'\| (unit/\S+) \| (\d+) \| (\d+) \| (\d+) \|',l)
        if m: d[m[1]]=tuple(map(int,m.groups()[1:]))
    return d
def fails(p):
    s=open(p,encoding='utf8').read()
    return set(re.findall(r'^\[err\]: (.*)$',s,re.M))
cols=sys.argv[1:]
ds=[load(c+'/summary.md') for c in cols]; fs=[fails(c+'/summary.md') for c in cols]
units=sorted(set().union(*ds))
print('| unit | '+' | '.join(c.split('/')[-1] for c in cols)+' |')
print('|---|'+'---:|'*len(cols))
for u in units:
    row=[ds[i].get(u) for i in range(len(cols))]
    if len(set(row))>1 or any(r and (r[1] or r[2]) for r in row):
        print(f'| {u} | '+' | '.join(f'{r[0]}/{r[1]}/{r[2]}' if r else '-' for r in row)+' |')
tot=[tuple(sum(x[j] for x in d.values()) for j in range(3)) for d in ds]
print('| **total** (passed/failed/aborted) | '+' | '.join(f'{t[0]}/{t[1]}/{t[2]}' for t in tot)+' |')
print(f'units: {[len(d) for d in ds]}')
base=fs[0]
for i in range(1,len(cols)):
    print(f'\nfails in {cols[i]} not in {cols[0]}:'); [print('  NEW', x) for x in sorted(fs[i]-base)]
    print(f'fails in {cols[0]} not in {cols[i]}:'); [print('  GONE', x) for x in sorted(base-fs[i])]
